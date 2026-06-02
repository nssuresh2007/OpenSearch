/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendAggregationExecutor;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.startree.StarTreeQueryHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Walks the parsed {@link AggregatorFactories} tree and classifies each top-level
 * aggregation as eligible (delegated to a chosen backend) or ineligible (runs on
 * the standard Lucene aggregator path).
 *
 * <p>The classifier is stateless and pure — the same inputs always produce the same
 * output. Caching is handled externally by {@link EligibilityCache}.
 *
 * @opensearch.internal
 */
public class AggregationEligibilityClassifier {

    private final CapabilityRegistry capabilityRegistry;
    private final Function<String, FieldStorageInfo> fieldStorageProvider;
    private final QueryShardContext queryShardContext;

    /**
     * @param capabilityRegistry   the registry for backend capability lookups
     * @param fieldStorageProvider  resolves a field name to its {@link FieldStorageInfo}; returns null if unknown
     * @param queryShardContext    the shard's query context (used for star-tree availability check)
     */
    public AggregationEligibilityClassifier(
        CapabilityRegistry capabilityRegistry,
        Function<String, FieldStorageInfo> fieldStorageProvider,
        QueryShardContext queryShardContext
    ) {
        this.capabilityRegistry = capabilityRegistry;
        this.fieldStorageProvider = fieldStorageProvider;
        this.queryShardContext = queryShardContext;
    }

    /**
     * Classify all top-level aggregations in the given factories builder.
     *
     * @param factories the parsed aggregation tree from the search request
     * @return an {@link EligibilitySplit} with disjoint eligible/ineligible subsets
     */
    public EligibilitySplit classify(AggregatorFactories.Builder factories) {
        Collection<AggregationBuilder> topLevel = factories.getAggregatorFactories();
        List<EligibleAggregation> eligible = new ArrayList<>();
        AggregatorFactories.Builder ineligible = AggregatorFactories.builder();
        Map<String, IneligibilityReason> reasons = new HashMap<>();

        for (AggregationBuilder agg : topLevel) {
            ClassifyResult result = classifyRecursive(agg);
            if (result.eligible) {
                eligible.add(new EligibleAggregation(agg, result.chosenBackendId));
            } else {
                ineligible.addAggregator(agg);
                reasons.put(agg.getName(), result.reason);
            }
        }

        return new EligibilitySplit(eligible, ineligible, reasons);
    }

    private ClassifyResult classifyRecursive(AggregationBuilder agg) {
        // 1. Star-tree available → Ineligible (Req 20.1)
        if (StarTreeQueryHelper.getSupportedStarTree(queryShardContext) != null) {
            return ClassifyResult.ineligible(IneligibilityReason.STAR_TREE_AVAILABLE);
        }

        // 2. Script → Ineligible (Req 2.4)
        if (agg instanceof ValuesSourceAggregationBuilder<?> vsAgg) {
            if (vsAgg.script() != null) {
                return ClassifyResult.ineligible(IneligibilityReason.USES_SCRIPT);
            }
        }

        // 3. Map aggregation type to AggregateFunction
        AggregateFunction function = mapAggregationType(agg.getType());
        if (function == null) {
            return ClassifyResult.ineligible(IneligibilityReason.UNSUPPORTED_AGGREGATION_TYPE);
        }

        // 4. For each field referenced, find capable backends
        String chosenBackend = null;
        if (agg instanceof ValuesSourceAggregationBuilder<?> vsAgg) {
            String fieldName = vsAgg.field();
            if (fieldName != null) {
                FieldStorageInfo info = fieldStorageProvider.apply(fieldName);
                if (info == null) {
                    return ClassifyResult.ineligible(IneligibilityReason.NO_PARQUET_DOC_VALUES);
                }
                if (info.getDocValueFormats() == null || info.getDocValueFormats().stream().noneMatch("parquet"::equals)) {
                    return ClassifyResult.ineligible(IneligibilityReason.NO_PARQUET_DOC_VALUES);
                }

                List<String> candidates = capabilityRegistry.aggregateBackendsForField(function, info);
                // Filter to backends with non-null aggregationExecutor() and canEvaluate
                candidates = filterToExecutableBackends(candidates, agg, info);
                if (candidates.isEmpty()) {
                    return ClassifyResult.ineligible(IneligibilityReason.NO_CAPABLE_BACKEND);
                }

                String selected = selectBackend(candidates, info);
                if (chosenBackend != null && chosenBackend.equals(selected) == false) {
                    return ClassifyResult.ineligible(IneligibilityReason.SUB_AGGREGATION_DIFFERENT_BACKEND);
                }
                chosenBackend = selected;
            }
            // field == null means count(*) style — any backend with aggregate capabilities can handle it
            if (fieldName == null && chosenBackend == null) {
                List<String> allCapable = filterToExecutableBackends(
                    new ArrayList<>(capabilityRegistry.aggregateCapableBackends()),
                    agg,
                    null
                );
                if (allCapable.isEmpty()) {
                    return ClassifyResult.ineligible(IneligibilityReason.NO_CAPABLE_BACKEND);
                }
                chosenBackend = allCapable.getFirst();
            }
        } else {
            // Non-ValuesSource aggregation (e.g. filters, composite) — check if any backend can handle
            // For now, treat non-ValuesSource aggs as needing special handling per type
            return ClassifyResult.ineligible(IneligibilityReason.UNSUPPORTED_AGGREGATION_TYPE);
        }

        // 5. Recurse into sub-aggregations (Req 2.6)
        for (AggregationBuilder subAgg : agg.getSubAggregations()) {
            ClassifyResult subResult = classifyRecursive(subAgg);
            if (subResult.eligible == false) {
                return ClassifyResult.ineligible(IneligibilityReason.SUB_AGGREGATION_INELIGIBLE);
            }
            if (chosenBackend != null && chosenBackend.equals(subResult.chosenBackendId) == false) {
                return ClassifyResult.ineligible(IneligibilityReason.SUB_AGGREGATION_DIFFERENT_BACKEND);
            }
            if (chosenBackend == null) {
                chosenBackend = subResult.chosenBackendId;
            }
        }

        if (chosenBackend == null) {
            return ClassifyResult.ineligible(IneligibilityReason.NO_CAPABLE_BACKEND);
        }

        return ClassifyResult.eligible(chosenBackend);
    }

    /**
     * Filter candidate backend names to those that have a non-null {@code aggregationExecutor()}
     * and whose executor's {@code canEvaluate} returns true.
     */
    private List<String> filterToExecutableBackends(List<String> candidates, AggregationBuilder agg, FieldStorageInfo field) {
        List<String> result = new ArrayList<>();
        for (String name : candidates) {
            AnalyticsSearchBackendPlugin backend = capabilityRegistry.getBackend(name);
            BackendAggregationExecutor executor = backend.aggregationExecutor();
            if (executor == null) {
                continue;
            }
            if (field == null || executor.canEvaluate(agg, field)) {
                result.add(name);
            }
        }
        return result;
    }

    /**
     * Deterministic backend selection rule:
     * 1. Format affinity — prefer backends whose capabilities include the field's primary format
     * 2. Priority — higher {@code aggregationPriority()} wins
     * 3. Alphabetical — tie-break by backend id ascending
     */
    private String selectBackend(List<String> candidates, FieldStorageInfo field) {
        if (candidates.size() == 1) {
            return candidates.getFirst();
        }

        String primaryFormat = (field.getDocValueFormats() != null && field.getDocValueFormats().isEmpty() == false)
            ? field.getDocValueFormats().getFirst()
            : null;

        return candidates.stream().min(Comparator.<String, Boolean>comparing(name -> {
            // Format affinity: backends matching primary format sort first (false < true inverted)
            if (primaryFormat == null) return false;
            AnalyticsSearchBackendPlugin backend = capabilityRegistry.getBackend(name);
            BackendCapabilityProvider provider = backend.getCapabilityProvider();
            return provider.aggregateCapabilities().stream().noneMatch(cap -> cap.formats().contains(primaryFormat));
        }).thenComparing(name -> {
            // Priority: higher is better, so negate for ascending sort
            AnalyticsSearchBackendPlugin backend = capabilityRegistry.getBackend(name);
            return -backend.getCapabilityProvider().aggregationPriority();
        }).thenComparing(name -> name) // Alphabetical tie-break
        ).orElse(candidates.getFirst());
    }

    /**
     * Maps an OpenSearch aggregation type string to the analytics-framework's
     * {@link AggregateFunction} enum. Returns null if the type is not supported
     * for delegation.
     */
    static AggregateFunction mapAggregationType(String aggType) {
        return switch (aggType) {
            case "sum" -> AggregateFunction.SUM;
            case "avg" -> AggregateFunction.AVG;
            case "min" -> AggregateFunction.MIN;
            case "max" -> AggregateFunction.MAX;
            case "value_count", "count" -> AggregateFunction.COUNT;
            case "stats" -> AggregateFunction.SUM; // stats decomposes to sum/count/min/max
            case "extended_stats" -> AggregateFunction.SUM; // same decomposition
            case "cardinality" -> AggregateFunction.APPROX_COUNT_DISTINCT;
            case "percentiles" -> AggregateFunction.PERCENTILE_APPROX;
            case "terms", "sterms", "lterms", "dterms" -> AggregateFunction.COUNT; // terms is group-by + count
            case "date_histogram" -> AggregateFunction.COUNT; // bucket agg with count
            case "histogram" -> AggregateFunction.COUNT;
            case "range" -> AggregateFunction.COUNT;
            case "filters" -> AggregateFunction.COUNT;
            default -> null;
        };
    }

    /**
     * Internal result of classifying a single aggregation node.
     */
    static final class ClassifyResult {
        final boolean eligible;
        final String chosenBackendId;
        final IneligibilityReason reason;

        private ClassifyResult(boolean eligible, String chosenBackendId, IneligibilityReason reason) {
            this.eligible = eligible;
            this.chosenBackendId = chosenBackendId;
            this.reason = reason;
        }

        static ClassifyResult eligible(String chosenBackendId) {
            return new ClassifyResult(true, chosenBackendId, null);
        }

        static ClassifyResult ineligible(IneligibilityReason reason) {
            return new ClassifyResult(false, null, reason);
        }
    }
}
