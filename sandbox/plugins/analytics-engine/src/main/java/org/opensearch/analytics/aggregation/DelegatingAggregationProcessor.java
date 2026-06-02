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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

/**
 * An {@link AggregationProcessor} that intercepts the aggregation phase to delegate
 * eligible aggregations to a backend executor (e.g. DataFusion) while letting ineligible
 * aggregations run through the standard Lucene aggregator path.
 *
 * <p>This processor wraps a delegate (typically {@code DefaultAggregationProcessor} or
 * {@code ConcurrentAggregationProcessor}) and adds two capabilities:
 * <ol>
 *   <li>In {@link #preProcess}: classifies aggregations using {@link AggregationEligibilityClassifier},
 *       registers Lucene aggregators for ineligible ones via the delegate, and registers a
 *       {@link DocIdBitsetCollectorManager} for eligible ones to collect per-segment matching docIds.</li>
 *   <li>In {@link #postProcess}: calls the delegate's postProcess (producing Lucene-side results for
 *       ineligible aggregations), pulls bitsets from the collector manager, calls
 *       {@link AggregationDelegationService#execute} for eligible aggregations, and merges both
 *       result sets in original request order.</li>
 * </ol>
 *
 * <p>Non-aggregation context (query, post-filter, sort, source filtering, hits) is never modified
 * by this processor (Req 19).
 *
 * @opensearch.internal
 */
public class DelegatingAggregationProcessor implements AggregationProcessor {

    private static final Logger logger = LogManager.getLogger(DelegatingAggregationProcessor.class);

    private final AggregationProcessor delegate;
    private final CapabilityRegistry capabilityRegistry;
    private final Function<String, FieldStorageInfo> fieldStorageProvider;
    private final AggregationDelegationService delegationService;
    private final LongUnaryOperator writerGenerationLookup;

    /**
     * Creates a new delegating aggregation processor.
     *
     * @param delegate                the underlying processor for ineligible aggregations (and global aggs)
     * @param capabilityRegistry      the registry for backend capability lookups
     * @param fieldStorageProvider    resolves a field name to its {@link FieldStorageInfo}
     * @param delegationService       the service that orchestrates backend execution
     * @param writerGenerationLookup  maps a segment ordinal to the composite-engine writer generation
     */
    public DelegatingAggregationProcessor(
        AggregationProcessor delegate,
        CapabilityRegistry capabilityRegistry,
        Function<String, FieldStorageInfo> fieldStorageProvider,
        AggregationDelegationService delegationService,
        LongUnaryOperator writerGenerationLookup
    ) {
        this.delegate = delegate;
        this.capabilityRegistry = capabilityRegistry;
        this.fieldStorageProvider = fieldStorageProvider;
        this.delegationService = delegationService;
        this.writerGenerationLookup = writerGenerationLookup;
    }

    /**
     * Classifies aggregations and registers appropriate collectors.
     *
     * <p>If there are no aggregations, delegates straight to the default processor (Req 1.2).
     * Otherwise:
     * <ol>
     *   <li>Walks the parsed aggregation tree through {@link AggregationEligibilityClassifier}</li>
     *   <li>For ineligible aggregations: delegates to the default processor's preProcess so standard
     *       {@code NonGlobalAggCollectorManager} is registered</li>
     *   <li>For eligible aggregations: registers a {@link DocIdBitsetCollectorManager} so its collector
     *       receives every matching doc per segment during the query phase</li>
     *   <li>Creates a {@link DelegationRequestState} and registers it as a releasable on the context</li>
     * </ol>
     */
    @Override
    public void preProcess(SearchContext context) {
        // No aggregations → delegate straight to the default processor (Req 1.2)
        if (context.aggregations() == null) {
            logger.info("[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.preProcess: no aggregations, delegating to default");
            delegate.preProcess(context);
            return;
        }

        AggregatorFactories.Builder factoriesBuilder = context.request().source() != null
            ? context.request().source().aggregations()
            : null;

        // No aggregation builders in the source → delegate to default
        if (factoriesBuilder == null || factoriesBuilder.getAggregatorFactories().isEmpty()) {
            logger.info("[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.preProcess: no agg builders in source, delegating to default");
            delegate.preProcess(context);
            return;
        }

        // Classify the aggregation tree
        AggregationEligibilityClassifier classifier = new AggregationEligibilityClassifier(
            capabilityRegistry,
            fieldStorageProvider,
            context.getQueryShardContext()
        );
        EligibilitySplit split = classifier.classify(factoriesBuilder);

        // If nothing is eligible, delegate entirely to the default processor
        if (split.eligible().isEmpty()) {
            logger.info(
                "[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.preProcess: NO eligible aggregations. Reasons: {}. Falling back to default Lucene path.",
                split.reasons()
            );
            delegate.preProcess(context);
            return;
        }

        // Log classification results
        logger.info(
            "[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.preProcess: {} ELIGIBLE ({}), {} ineligible. Reasons: {}",
            split.eligible().size(),
            split.eligible().stream().map(e -> e.agg().getName() + "→" + e.chosenBackendId()).toList(),
            split.ineligible().getAggregatorFactories().size(),
            split.reasons()
        );

        // For ineligible aggregations: let the delegate register Lucene aggregators.
        // If ALL aggregations are eligible, skip the delegate entirely — there's nothing
        // for Lucene to aggregate, and calling delegate.preProcess would try to build
        // Lucene Aggregator instances that read doc values (which may not exist in Lucene
        // for parquet-primary composite indexes).
        if (split.ineligible().getAggregatorFactories().isEmpty() == false) {
            delegate.preProcess(context);
        } else {
            logger.info("[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.preProcess: all aggs eligible, skipping delegate.preProcess (no Lucene aggregators needed)");
        }

        // For eligible aggregations: register a DocIdBitsetCollectorManager
        DocIdBitsetCollectorManager bitsetCollectorManager = new DocIdBitsetCollectorManager(context.query(), writerGenerationLookup);

        // Wrap in a CollectorManager that satisfies the queryCollectorManagers type signature
        BitsetCollectorManagerAdapter adapter = new BitsetCollectorManagerAdapter(bitsetCollectorManager);
        context.queryCollectorManagers().put(DocIdBitsetCollectorManager.class, adapter);

        // Create DelegationRequestState and register as releasable for cleanup
        DelegationRequestState state = new DelegationRequestState(bitsetCollectorManager, split.eligible());
        adapter.setDelegationRequestState(state);
        context.addReleasable(state);
    }

    /**
     * Executes backend delegation for eligible aggregations and merges results.
     *
     * <ol>
     *   <li>Calls the delegate's postProcess first — produces {@link InternalAggregations} for
     *       ineligible aggregations (Req 10)</li>
     *   <li>Pulls the per-segment {@code FixedBitSet} map from the {@link DocIdBitsetCollectorManager}</li>
     *   <li>Calls {@link AggregationDelegationService#execute} for eligible aggregations</li>
     *   <li>Merges both result sets in original request order and stamps into
     *       {@code ctx.queryResult().aggregations()}</li>
     * </ol>
     */
    @Override
    public void postProcess(SearchContext context) {
        if (context.aggregations() == null) {
            delegate.postProcess(context);
            return;
        }

        // Check if we have a registered bitset collector (i.e., there are eligible aggregations)
        BitsetCollectorManagerAdapter adapter = (BitsetCollectorManagerAdapter) context.queryCollectorManagers()
            .get(DocIdBitsetCollectorManager.class);

        if (adapter == null) {
            // No eligible aggregations were registered — just delegate
            delegate.postProcess(context);
            return;
        }

        // Step 1: Call delegate's postProcess to produce InternalAggregations for ineligible aggs
        delegate.postProcess(context);

        // Retrieve the ineligible aggregation results that the delegate produced
        InternalAggregations ineligibleResults = null;
        if (context.queryResult().hasAggs()) {
            ineligibleResults = context.queryResult().aggregations() != null ? context.queryResult().aggregations().expand() : null;
        }

        // Step 2: Pull the per-segment bitsets from the collector manager
        PerSegmentBitsets perSegmentBitsets = adapter.getCollectedBitsets();
        if (perSegmentBitsets == null || perSegmentBitsets.isEmpty()) {
            // No matching documents in any segment — eligible aggs produce empty results
            // The ineligible results from the delegate are already stamped; nothing more to do
            logger.info("[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.postProcess: no matching docs in bitsets; skipping backend delegation");
            cleanupCollectorManager(context);
            return;
        }

        // Step 3: Retrieve the DelegationRequestState to get eligible aggregations
        DelegationRequestState state = findDelegationState(adapter);
        if (state == null) {
            logger.warn("[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.postProcess: DelegationRequestState not found; skipping backend delegation");
            cleanupCollectorManager(context);
            return;
        }

        try {
            logger.info(
                "[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.postProcess: executing backend delegation for {} eligible aggs, {} segments with docs",
                state.getEligibleAggregations().size(),
                perSegmentBitsets.entries().size()
            );

            // Step 4: Call AggregationDelegationService.execute for eligible aggregations
            List<InternalAggregation> backendResults = delegationService.execute(
                context,
                state.getEligibleAggregations(),
                perSegmentBitsets,
                state
            );

            logger.info(
                "[AGG_DELEGATION_TRACE] DelegatingAggregationProcessor.postProcess: backend returned {} results: {}",
                backendResults.size(),
                backendResults.stream().map(r -> r.getName() + "=" + r).toList()
            );

            // Step 5: Merge ineligible (Lucene-side) and eligible (backend-side) results
            // in original request order
            List<InternalAggregation> mergedResults = mergeInOriginalOrder(
                context,
                ineligibleResults,
                backendResults,
                state.getEligibleAggregations()
            );

            // Stamp merged results into queryResult
            context.queryResult().aggregations(new InternalAggregations(mergedResults));

        } catch (IOException e) {
            throw new QueryPhaseExecutionException(context.shardTarget(), "Failed to execute delegated aggregations on backend", e);
        } finally {
            cleanupCollectorManager(context);
        }
    }

    /**
     * Merges ineligible (Lucene-side) and eligible (backend-side) aggregation results
     * in the original request order.
     *
     * <p>The original order is determined by the order of aggregation builders in the
     * search request. Eligible aggregations are placed at their original positions,
     * and ineligible aggregations fill the remaining positions.
     */
    private List<InternalAggregation> mergeInOriginalOrder(
        SearchContext context,
        InternalAggregations ineligibleResults,
        List<InternalAggregation> backendResults,
        List<EligibleAggregation> eligibleAggregations
    ) {
        AggregatorFactories.Builder factoriesBuilder = context.request().source().aggregations();
        if (factoriesBuilder == null) {
            return backendResults;
        }

        // Build a map of eligible aggregation names to their backend results
        Map<String, InternalAggregation> eligibleResultMap = new HashMap<>();
        for (int i = 0; i < eligibleAggregations.size() && i < backendResults.size(); i++) {
            eligibleResultMap.put(eligibleAggregations.get(i).agg().getName(), backendResults.get(i));
        }

        // Build a map of ineligible aggregation results by name
        Map<String, InternalAggregation> ineligibleResultMap = new HashMap<>();
        if (ineligibleResults != null) {
            for (Aggregation agg : ineligibleResults.asList()) {
                if (agg instanceof InternalAggregation internalAgg) {
                    ineligibleResultMap.put(internalAgg.getName(), internalAgg);
                }
            }
        }

        // Merge in original order
        List<InternalAggregation> merged = new ArrayList<>();
        for (AggregationBuilder builder : factoriesBuilder.getAggregatorFactories()) {
            String name = builder.getName();
            InternalAggregation result = eligibleResultMap.get(name);
            if (result == null) {
                result = ineligibleResultMap.get(name);
            }
            if (result != null) {
                merged.add(result);
            }
        }

        return merged;
    }

    /**
     * Finds the DelegationRequestState associated with the adapter.
     * The state is stored in the adapter during preProcess.
     */
    private DelegationRequestState findDelegationState(BitsetCollectorManagerAdapter adapter) {
        return adapter.getDelegationRequestState();
    }

    /**
     * Removes the bitset collector manager from the query collector managers map.
     */
    private void cleanupCollectorManager(SearchContext context) {
        context.queryCollectorManagers().remove(DocIdBitsetCollectorManager.class);
    }

    /**
     * Adapter that wraps a {@link DocIdBitsetCollectorManager} to satisfy the
     * {@code CollectorManager<? extends Collector, ReduceableSearchResult>} type signature
     * required by {@code SearchContext.queryCollectorManagers()}.
     *
     * <p>The adapter's {@link #reduce} method collects the per-segment bitsets and stores
     * them for later retrieval in {@link #postProcess}. It returns a no-op
     * {@link ReduceableSearchResult} since the actual result stamping is handled by
     * the {@link DelegatingAggregationProcessor#postProcess} method.
     *
     * @opensearch.internal
     */
    static class BitsetCollectorManagerAdapter implements CollectorManager<Collector, ReduceableSearchResult> {

        private final DocIdBitsetCollectorManager delegate;
        private volatile PerSegmentBitsets collectedBitsets;
        private DelegationRequestState delegationRequestState;

        BitsetCollectorManagerAdapter(DocIdBitsetCollectorManager delegate) {
            this.delegate = delegate;
        }

        void setDelegationRequestState(DelegationRequestState state) {
            this.delegationRequestState = state;
        }

        DelegationRequestState getDelegationRequestState() {
            return delegationRequestState;
        }

        @Override
        public Collector newCollector() throws IOException {
            return delegate.newCollector();
        }

        @Override
        @SuppressWarnings("unchecked")
        public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
            // Cast is safe because newCollector() always returns DocIdBitsetCollector
            Collection<DocIdBitsetCollectorManager.DocIdBitsetCollector> typedCollectors = (Collection<
                DocIdBitsetCollectorManager.DocIdBitsetCollector>) (Collection<?>) collectors;
            this.collectedBitsets = delegate.reduce(typedCollectors);

            // Return a no-op ReduceableSearchResult — we handle result stamping in postProcess
            return querySearchResult -> {
                // no-op: DelegatingAggregationProcessor.postProcess handles the result merging
            };
        }

        /**
         * Returns the collected per-segment bitsets after the query phase completes.
         * May be null if reduce() has not been called yet.
         */
        PerSegmentBitsets getCollectedBitsets() {
            return collectedBitsets;
        }
    }
}
