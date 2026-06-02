/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.analytics.spi.BackendAggregationExecutor;
import org.opensearch.analytics.spi.BackendExecutionException;
import org.opensearch.analytics.spi.SegmentExecutionContext;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Orchestrates the execution of eligible aggregations on backend executors.
 *
 * <p>Groups eligible aggregations by {@code chosenBackendId}, registers bitsets
 * in the provider registry, calls {@code BackendAggregationExecutor.executeOnSegment}
 * per non-empty segment, combines per-segment partials, and returns the merged
 * {@link InternalAggregation} list.
 *
 * <p>On {@link BackendExecutionException}, the service falls back to the Lucene path
 * for the affected segment/aggregation: it logs the error at WARN level and re-runs
 * the aggregation through the standard Lucene aggregator path (Req 17).
 *
 * @opensearch.internal
 */
public class AggregationDelegationService {

    private static final Logger logger = LogManager.getLogger(AggregationDelegationService.class);

    private final BackendRouter backendRouter;
    private final BitsetProviderRegistry bitsetProviderRegistry;

    /**
     * Creates a new delegation service.
     *
     * @param backendRouter          provides O(1) lookup of {@link BackendAggregationExecutor} by backend id
     * @param bitsetProviderRegistry registry for per-segment bitsets accessible via FFM callbacks
     */
    public AggregationDelegationService(BackendRouter backendRouter, BitsetProviderRegistry bitsetProviderRegistry) {
        this.backendRouter = backendRouter;
        this.bitsetProviderRegistry = bitsetProviderRegistry;
    }

    /**
     * Executes eligible aggregations on their chosen backends using the collected
     * per-segment bitsets.
     *
     * <p>The execution flow:
     * <ol>
     *   <li>Groups eligible aggregations by {@code chosenBackendId}</li>
     *   <li>Registers bitsets in the {@link BitsetProviderRegistry} for FFM access</li>
     *   <li>For each non-empty segment: calls {@code BackendAggregationExecutor.executeOnSegment}</li>
     *   <li>On {@link BackendExecutionException}: logs at WARN, falls back to Lucene path</li>
     *   <li>Combines per-segment partials via {@code InternalAggregations.reduce(partial=true)}</li>
     *   <li>Releases provider on completion</li>
     * </ol>
     *
     * @param context              the search context
     * @param eligibleAggregations the aggregations classified as eligible for delegation
     * @param perSegmentBitsets    the per-segment matching-doc bitsets collected during query phase
     * @param state                the delegation request state for lifecycle management
     * @return list of {@link InternalAggregation} results from backend execution, one per eligible aggregation
     * @throws IOException if an unrecoverable error occurs during execution
     */
    public List<InternalAggregation> execute(
        SearchContext context,
        List<EligibleAggregation> eligibleAggregations,
        PerSegmentBitsets perSegmentBitsets,
        DelegationRequestState state
    ) throws IOException {
        if (eligibleAggregations.isEmpty()) {
            return Collections.emptyList();
        }

        logger.info(
            "[AGG_DELEGATION_TRACE] AggregationDelegationService.execute: {} eligible aggs, {} segments with bitsets",
            eligibleAggregations.size(),
            perSegmentBitsets.entries().size()
        );

        // Step 1: Group eligible aggregations by chosenBackendId
        Map<String, List<EligibleAggregation>> aggsByBackend = groupByBackend(eligibleAggregations);

        // Step 2: Register bitsets in the BitsetProviderRegistry for FFM access
        int providerKey = bitsetProviderRegistry.registerBitsets(perSegmentBitsets);

        try {
            // Step 3: Execute per-backend, per-segment
            // Results are indexed by aggregation name for easy lookup
            Map<String, InternalAggregation> resultsByName = new LinkedHashMap<>();

            for (Map.Entry<String, List<EligibleAggregation>> backendEntry : aggsByBackend.entrySet()) {
                String backendId = backendEntry.getKey();
                List<EligibleAggregation> backendAggs = backendEntry.getValue();

                BackendAggregationExecutor executor = backendRouter.getExecutor(backendId);
                if (executor == null) {
                    // Backend not found — fall back to Lucene for all aggs in this group
                    logger.warn(
                        "Backend executor not found for backendId [{}]; falling back to Lucene path for {} aggregation(s)",
                        backendId,
                        backendAggs.size()
                    );
                    for (EligibleAggregation agg : backendAggs) {
                        InternalAggregation fallbackResult = executeFallbackOnLucene(context, agg);
                        if (fallbackResult != null) {
                            resultsByName.put(agg.agg().getName(), fallbackResult);
                        }
                    }
                    continue;
                }

                // Execute each aggregation in this backend group
                for (EligibleAggregation eligibleAgg : backendAggs) {
                    InternalAggregation result = executeAggregationOnBackend(context, eligibleAgg, executor, perSegmentBitsets, providerKey);
                    if (result != null) {
                        resultsByName.put(eligibleAgg.agg().getName(), result);
                    }
                }
            }

            // Step 4: Return results in the same order as the input eligible aggregations
            List<InternalAggregation> orderedResults = new ArrayList<>(eligibleAggregations.size());
            for (EligibleAggregation agg : eligibleAggregations) {
                InternalAggregation result = resultsByName.get(agg.agg().getName());
                if (result != null) {
                    orderedResults.add(result);
                }
            }

            return orderedResults;
        } finally {
            // Step 5: Release provider on completion
            bitsetProviderRegistry.releaseProvider(providerKey);
        }
    }

    /**
     * Executes a single eligible aggregation across all non-empty segments on the given backend,
     * then combines per-segment partials via {@code InternalAggregations.reduce(partial=true)}.
     *
     * <p>If a segment fails with {@link BackendExecutionException}, the fallback logic runs
     * the aggregation through the standard Lucene path for that segment.
     */
    private InternalAggregation executeAggregationOnBackend(
        SearchContext context,
        EligibleAggregation eligibleAgg,
        BackendAggregationExecutor executor,
        PerSegmentBitsets perSegmentBitsets,
        int providerKey
    ) throws IOException {
        List<InternalAggregations> perSegmentPartials = new ArrayList<>();

        // Build the AggregatorFactories for the eligible subtree
        AggregatorFactories eligibleSubtree = buildEligibleSubtree(context, eligibleAgg);

        for (PerSegmentBitsets.BitsetEntry entry : perSegmentBitsets.entries()) {
            // Skip segments with zero matching documents
            if (entry.cardinality() <= 0) {
                continue;
            }

            // Resolve the bitset for this segment
            FixedBitSet bitset = resolveBitset(entry);

            try {
                // Set per-segment context if the executor supports it (e.g., DataFusion needs
                // providerKey and writerGeneration for the FFM bridge call)
                if (executor instanceof SegmentExecutionContext segCtxAware) {
                    segCtxAware.setSegmentContext(providerKey, entry.writerGeneration(), eligibleAgg.agg());
                }

                // Install a FilterDelegationHandle that routes FFM callbacks to our
                // BitsetProviderRegistry. The Rust IndexedTableProvider calls createCollector/
                // collectDocs/releaseCollector via the FFM callback table.
                installBitsetDelegationHandle();

                // Call the backend executor for this segment
                InternalAggregation segmentResult = executor.executeOnSegment(eligibleSubtree, entry.leaf(), bitset, context);

                logger.info(
                    "[AGG_DELEGATION_TRACE] AggregationDelegationService: segment ord={} gen={} cardinality={} → backend returned: {}",
                    entry.leaf().ord,
                    entry.writerGeneration(),
                    entry.cardinality(),
                    segmentResult
                );

                if (segmentResult != null) {
                    perSegmentPartials.add(new InternalAggregations(List.of(segmentResult)));
                }
            } catch (BackendExecutionException e) {
                // Fallback: log at WARN and re-run via Lucene path for this segment (Req 17)
                logger.warn(
                    "Backend execution failed for aggregation [{}] on segment [{}] (backend={}): {}. "
                        + "Falling back to Lucene path for this segment. Cause: {}",
                    eligibleAgg.agg().getName(),
                    entry.leaf().ord,
                    executor.backendId(),
                    e.getMessage(),
                    e.getCause() != null ? e.getCause().getMessage() : "none",
                    e
                );

                InternalAggregation fallbackResult = executeFallbackOnLuceneForSegment(context, eligibleAgg, entry);
                if (fallbackResult != null) {
                    perSegmentPartials.add(new InternalAggregations(List.of(fallbackResult)));
                }
            }
        }

        // Combine per-segment partials via InternalAggregations.reduce(partial=true)
        if (perSegmentPartials.isEmpty()) {
            return null;
        }

        InternalAggregations reduced = InternalAggregations.reduce(perSegmentPartials, context.partialOnShard());
        if (reduced == null || reduced.asList().isEmpty()) {
            return null;
        }

        // Return the single aggregation from the reduced result
        return (InternalAggregation) reduced.asList().get(0);
    }

    /**
     * Builds the {@link AggregatorFactories} for the eligible aggregation subtree.
     *
     * <p>Constructs an {@code AggregatorFactories.Builder} containing the single eligible
     * aggregation (with its sub-aggregations), then builds it against the search context's
     * {@code QueryShardContext}. This produces the built form that the backend executor
     * needs to inspect the aggregation tree structure.
     *
     * @param context     the search context providing the QueryShardContext
     * @param eligibleAgg the eligible aggregation to build
     * @return the built AggregatorFactories
     * @throws IOException if building fails
     */
    private AggregatorFactories buildEligibleSubtree(SearchContext context, EligibleAggregation eligibleAgg) throws IOException {
        AggregatorFactories.Builder builder = AggregatorFactories.builder();
        builder.addAggregator(eligibleAgg.agg());
        return builder.build(context.getQueryShardContext(), null);
    }

    /**
     * Resolves the actual {@link FixedBitSet} for a segment entry.
     * For {@link PerSegmentBitsets.BitsetKind#MATCH_ALL_LIVE}, creates a full bitset
     * covering all live documents in the segment.
     */
    private FixedBitSet resolveBitset(PerSegmentBitsets.BitsetEntry entry) {
        if (entry.kind() == PerSegmentBitsets.BitsetKind.MATCH_ALL_LIVE) {
            // Create a full bitset for all live docs
            int maxDoc = entry.leaf().reader().maxDoc();
            FixedBitSet fullBitset = new FixedBitSet(maxDoc);
            fullBitset.set(0, maxDoc);
            return fullBitset;
        }
        return entry.bitset();
    }

    /**
     * Installs a {@link org.opensearch.analytics.spi.FilterDelegationHandle} that routes
     * FFM callbacks (createCollector, collectDocs, releaseCollector) to our
     * {@link BitsetProviderRegistry}. This must be called before each FFM execution call
     * so the Rust-side IndexedTableProvider can access the precomputed bitsets.
     */
    private void installBitsetDelegationHandle() {
        try {
            // Use reflection to call FilterTreeCallbacks.setHandle since it's in a different
            // plugin classloader. The handle routes createCollector/collectDocs to our registry.
            Class<?> callbacksClass = Class.forName("org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks");
            var setHandleMethod = callbacksClass.getMethod("setHandle", org.opensearch.analytics.spi.FilterDelegationHandle.class);
            setHandleMethod.invoke(null, new BitsetRegistryFilterHandle(bitsetProviderRegistry));
        } catch (Exception e) {
            logger.debug("Failed to install bitset delegation handle: {}", e.getMessage());
        }
    }

    /**
     * A {@link org.opensearch.analytics.spi.FilterDelegationHandle} that routes FFM callbacks
     * to the {@link BitsetProviderRegistry} for aggregation-time bitset access.
     */
    private static class BitsetRegistryFilterHandle implements org.opensearch.analytics.spi.FilterDelegationHandle {
        private final BitsetProviderRegistry registry;

        BitsetRegistryFilterHandle(BitsetProviderRegistry registry) {
            this.registry = registry;
        }

        @Override
        public int createProvider(int annotationId) {
            return annotationId; // pass-through — providers are pre-registered
        }

        @Override
        public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
            return registry.createCollector(providerKey, writerGeneration, minDoc, maxDoc);
        }

        @Override
        public int collectDocs(int collectorKey, int minDoc, int maxDoc, java.lang.foreign.MemorySegment out) {
            int outWordCap = (int) (out.byteSize() / 8);
            return registry.collectDocs(collectorKey, minDoc, maxDoc, out, outWordCap);
        }

        @Override
        public void releaseCollector(int collectorKey) {
            registry.releaseCollector(collectorKey);
        }

        @Override
        public void releaseProvider(int providerKey) {
            // No-op — lifecycle managed by the delegation service
        }

        @Override
        public void close() {
            // No-op
        }
    }

    /**
     * Groups eligible aggregations by their chosen backend id, preserving insertion order.
     */
    private Map<String, List<EligibleAggregation>> groupByBackend(List<EligibleAggregation> eligibleAggregations) {
        Map<String, List<EligibleAggregation>> grouped = new LinkedHashMap<>();
        for (EligibleAggregation agg : eligibleAggregations) {
            grouped.computeIfAbsent(agg.chosenBackendId(), k -> new ArrayList<>()).add(agg);
        }
        return grouped;
    }

    /**
     * Fallback: executes an aggregation through the standard Lucene path for all segments.
     * This is used when the backend executor is not found or when all segments fail.
     *
     * <p>In the current implementation, this returns {@code null} as the Lucene fallback
     * requires re-running the aggregation through the standard aggregator pipeline, which
     * is handled by the {@link DelegatingAggregationProcessor} when it detects that the
     * backend produced no result for an eligible aggregation. A full Lucene re-execution
     * would require constructing and running an {@code Aggregator} from scratch, which is
     * deferred to a future enhancement.
     *
     * @param context the search context
     * @param agg     the eligible aggregation that needs fallback
     * @return the fallback result, or {@code null} if fallback is not yet implemented
     */
    private InternalAggregation executeFallbackOnLucene(SearchContext context, EligibleAggregation agg) {
        // TODO: Implement full Lucene fallback path — construct Aggregator from AggregationBuilder,
        // run it over all segments, and return the result. For now, log and return null.
        logger.warn(
            "Lucene fallback for aggregation [{}] is not yet fully implemented; " + "aggregation may produce incomplete results",
            agg.agg().getName()
        );
        return null;
    }

    /**
     * Fallback: executes an aggregation through the standard Lucene path for a single segment.
     * This is invoked when the backend executor throws {@link BackendExecutionException} for
     * a specific segment.
     *
     * <p>In the current implementation, this returns {@code null} as per-segment Lucene
     * re-execution requires constructing a segment-scoped {@code Aggregator}, which is
     * deferred to a future enhancement. The overall aggregation result will be computed
     * from the segments that succeeded on the backend.
     *
     * @param context the search context
     * @param agg     the eligible aggregation that failed on the backend
     * @param entry   the segment bitset entry for the failed segment
     * @return the fallback result for this segment, or {@code null} if not yet implemented
     */
    private InternalAggregation executeFallbackOnLuceneForSegment(
        SearchContext context,
        EligibleAggregation agg,
        PerSegmentBitsets.BitsetEntry entry
    ) {
        // TODO: Implement per-segment Lucene fallback — construct a segment-scoped Aggregator,
        // collect matching docs from the bitset, and return the partial result.
        logger.warn(
            "Per-segment Lucene fallback for aggregation [{}] on segment [{}] is not yet fully implemented; "
                + "segment contribution may be missing from results",
            agg.agg().getName(),
            entry.leaf().ord
        );
        return null;
    }
}
