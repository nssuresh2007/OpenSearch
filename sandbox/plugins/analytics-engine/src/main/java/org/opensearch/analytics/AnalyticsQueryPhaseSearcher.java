/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.opensearch.analytics.aggregation.AggregationDelegationService;
import org.opensearch.analytics.aggregation.BackendRouter;
import org.opensearch.analytics.aggregation.BitsetProviderRegistry;
import org.opensearch.analytics.aggregation.DelegatingAggregationProcessor;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IndexSettings;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.ConcurrentQueryPhaseSearcher;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QueryPhaseSearcher;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * Custom {@link QueryPhaseSearcher} that intercepts the aggregation phase on opted-in
 * shards (composite engine + parquet field + capable backend) and returns a
 * {@link DelegatingAggregationProcessor} instead of the default processor.
 *
 * <p>For shards that are not opted-in, this searcher delegates entirely to the default
 * search behavior, returning the standard aggregation processor depending on whether
 * concurrent search is enabled.
 *
 * <p>The opt-in check is:
 * <ol>
 *   <li>The index setting {@code index.composite.primary_data_format} resolves to a non-lucene format
 *       (indicating composite engine is active)</li>
 *   <li>At least one field in the index has {@code "parquet"} in its
 *       {@link FieldStorageInfo#getDocValueFormats()}</li>
 *   <li>At least one installed backend has a non-null aggregation executor</li>
 * </ol>
 *
 * @opensearch.internal
 */
public class AnalyticsQueryPhaseSearcher implements QueryPhaseSearcher {

    private static final Logger logger = LogManager.getLogger(AnalyticsQueryPhaseSearcher.class);

    private static final String PARQUET_FORMAT = "parquet";
    private static final String LUCENE_FORMAT = "lucene";

    /**
     * The setting key for the primary data format in composite-engine indices.
     * Mirrors {@code CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT} without requiring
     * a compile-time dependency on the composite-engine plugin.
     */
    private static final String PRIMARY_DATA_FORMAT_SETTING = "index.composite.primary_data_format";

    private final QueryPhaseSearcher defaultSearcher;
    private final QueryPhaseSearcher concurrentSearcher;
    private final CapabilityRegistry capabilityRegistry;
    private final BackendRouter backendRouter;
    private final BitsetProviderRegistry bitsetProviderRegistry;
    private final AggregationDelegationService delegationService;

    /**
     * Creates a new analytics query phase searcher.
     *
     * @param capabilityRegistry      the registry for backend capability lookups
     * @param backendRouter           the router for backend executor lookups
     * @param bitsetProviderRegistry  the registry for per-segment bitsets accessible via FFM
     * @param delegationService       the service that orchestrates backend execution
     */
    public AnalyticsQueryPhaseSearcher(
        CapabilityRegistry capabilityRegistry,
        BackendRouter backendRouter,
        BitsetProviderRegistry bitsetProviderRegistry,
        AggregationDelegationService delegationService
    ) {
        this.defaultSearcher = QueryPhase.DEFAULT_QUERY_PHASE_SEARCHER;
        this.concurrentSearcher = new ConcurrentQueryPhaseSearcher();
        this.capabilityRegistry = capabilityRegistry;
        this.backendRouter = backendRouter;
        this.bitsetProviderRegistry = bitsetProviderRegistry;
        this.delegationService = delegationService;
    }

    @Override
    public boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        if (searchContext.shouldUseConcurrentSearch()) {
            return concurrentSearcher.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        } else {
            return defaultSearcher.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        // Get the default processor based on concurrent search mode
        AggregationProcessor defaultProcessor;
        if (searchContext.shouldUseConcurrentSearch()) {
            defaultProcessor = concurrentSearcher.aggregationProcessor(searchContext);
        } else {
            defaultProcessor = defaultSearcher.aggregationProcessor(searchContext);
        }

        // Check if this shard is opted-in for aggregation delegation
        if (isOptedIn(searchContext) == false) {
            logger.info("[AGG_DELEGATION_TRACE] AnalyticsQueryPhaseSearcher: shard [{}] NOT opted-in, using default processor",
                searchContext.indexShard().shardId());
            return defaultProcessor;
        }

        // Build the field storage provider from the index metadata
        Function<String, FieldStorageInfo> fieldStorageProvider = buildFieldStorageProvider(searchContext);
        if (fieldStorageProvider == null) {
            // Could not resolve field storage — fall back to default
            logger.info("[AGG_DELEGATION_TRACE] AnalyticsQueryPhaseSearcher: shard [{}] field storage provider is null, falling back",
                searchContext.indexShard().shardId());
            return defaultProcessor;
        }

        logger.info(
            "[AGG_DELEGATION_TRACE] AnalyticsQueryPhaseSearcher: shard [{}] opted-in for aggregation delegation; using DelegatingAggregationProcessor",
            searchContext.indexShard().shardId()
        );

        // Build the writerGenerationLookup from the Lucene segment attributes.
        // Each Lucene segment has a WRITER_GENERATION_ATTRIBUTE stamped by LuceneWriter at flush time.
        // We read it from the SegmentReader's SegmentInfo to map segment ordinal → writer generation.
        java.util.function.LongUnaryOperator writerGenerationLookup = segOrd -> {
            try {
                var leaves = searchContext.searcher().getIndexReader().leaves();
                if (segOrd < 0 || segOrd >= leaves.size()) return 0L;
                var leafReader = leaves.get((int) segOrd).reader();
                // Unwrap any FilterLeafReader wrappers (ExitableLeafReader, etc.) to get the SegmentReader
                while (leafReader instanceof org.apache.lucene.index.FilterLeafReader filterReader) {
                    leafReader = filterReader.getDelegate();
                }
                if (leafReader instanceof org.apache.lucene.index.SegmentReader segReader) {
                    String genAttr = segReader.getSegmentInfo().info.getAttribute("writer_generation");
                    if (genAttr != null) {
                        return Long.parseLong(genAttr);
                    }
                }
            } catch (Exception e) {
                logger.debug("Failed to read writer_generation for segment ord={}: {}", segOrd, e.getMessage());
            }
            return 0L;
        };

        // Return a DelegatingAggregationProcessor wrapping the default
        return new DelegatingAggregationProcessor(
            defaultProcessor,
            capabilityRegistry,
            fieldStorageProvider,
            delegationService,
            writerGenerationLookup
        );
    }

    /**
     * Checks whether the shard is opted-in for aggregation delegation.
     *
     * <p>The three conditions are:
     * <ol>
     *   <li>The index uses a composite engine (primary data format is not "lucene")</li>
     *   <li>At least one field has "parquet" in its doc value formats</li>
     *   <li>At least one backend has a non-null aggregation executor</li>
     * </ol>
     *
     * @param searchContext the search context for the current request
     * @return {@code true} if all three conditions are met
     */
    private boolean isOptedIn(SearchContext searchContext) {
        // Condition 1: Check if the index uses composite engine (primary format != "lucene")
        IndexSettings indexSettings = searchContext.indexShard().indexSettings();
        String primaryFormat = indexSettings.getSettings().get(PRIMARY_DATA_FORMAT_SETTING, LUCENE_FORMAT);
        if (LUCENE_FORMAT.equals(primaryFormat)) {
            logger.info("[AGG_DELEGATION_TRACE] isOptedIn: FAILED condition 1 — primary_data_format is 'lucene' for shard [{}]",
                searchContext.indexShard().shardId());
            return false;
        }

        // Condition 2: Check if at least one field has "parquet" in its doc value formats
        if (hasParquetDocValues(searchContext) == false) {
            logger.info("[AGG_DELEGATION_TRACE] isOptedIn: FAILED condition 2 — no field has 'parquet' doc values for shard [{}]",
                searchContext.indexShard().shardId());
            return false;
        }

        // Condition 3: Check if at least one backend has a non-null aggregation executor
        if (backendRouter.size() == 0) {
            logger.info("[AGG_DELEGATION_TRACE] isOptedIn: FAILED condition 3 — no backend has aggregation executor (backendRouter.size()=0) for shard [{}]",
                searchContext.indexShard().shardId());
            return false;
        }

        logger.info("[AGG_DELEGATION_TRACE] isOptedIn: ALL conditions passed for shard [{}] (primaryFormat={}, backendRouter.size={})",
            searchContext.indexShard().shardId(), primaryFormat, backendRouter.size());

        return true;
    }

    /**
     * Checks whether at least one field in the index has "parquet" in its doc value formats.
     *
     * <p>Uses the index metadata to resolve field storage info and checks each field's
     * doc value formats for the "parquet" format string.
     */
    private boolean hasParquetDocValues(SearchContext searchContext) {
        try {
            IndexMetadata indexMetadata = searchContext.indexShard().indexSettings().getIndexMetadata();
            FieldStorageResolver resolver = new FieldStorageResolver(indexMetadata);

            // Get all field names from the mapping and check if any has parquet doc values
            List<String> fieldNames = getAllFieldNames(indexMetadata);
            for (String fieldName : fieldNames) {
                try {
                    List<FieldStorageInfo> infos = resolver.resolve(List.of(fieldName));
                    if (infos.isEmpty() == false) {
                        FieldStorageInfo info = infos.get(0);
                        if (info.getDocValueFormats().contains(PARQUET_FORMAT)) {
                            return true;
                        }
                    }
                } catch (IllegalStateException e) {
                    // Field not found or unrecognized type — skip
                    logger.trace("Skipping field [{}] during parquet check: {}", fieldName, e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to check parquet doc values for shard [{}]: {}", searchContext.indexShard().shardId(), e.getMessage());
        }
        return false;
    }

    /**
     * Extracts all field names from the index metadata's mapping.
     */
    @SuppressWarnings("unchecked")
    private List<String> getAllFieldNames(IndexMetadata indexMetadata) {
        List<String> fieldNames = new java.util.ArrayList<>();
        if (indexMetadata.mapping() == null) {
            return fieldNames;
        }
        Object properties = indexMetadata.mapping().sourceAsMap().get("properties");
        if (properties instanceof java.util.Map) {
            collectFieldNames((java.util.Map<String, Object>) properties, "", fieldNames);
        }
        return fieldNames;
    }

    @SuppressWarnings("unchecked")
    private void collectFieldNames(java.util.Map<String, Object> properties, String prefix, List<String> fieldNames) {
        for (java.util.Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            if (entry.getValue() instanceof java.util.Map) {
                java.util.Map<String, Object> fieldProps = (java.util.Map<String, Object>) entry.getValue();
                if (fieldProps.containsKey("type")) {
                    fieldNames.add(fieldName);
                }
                // Recurse into nested properties
                Object nested = fieldProps.get("properties");
                if (nested instanceof java.util.Map) {
                    collectFieldNames((java.util.Map<String, Object>) nested, fieldName, fieldNames);
                }
            }
        }
    }

    /**
     * Builds a field storage provider function from the search context's index metadata.
     *
     * @return a function that maps field names to {@link FieldStorageInfo}, or {@code null}
     *         if the resolver cannot be constructed
     */
    private Function<String, FieldStorageInfo> buildFieldStorageProvider(SearchContext searchContext) {
        try {
            IndexMetadata indexMetadata = searchContext.indexShard().indexSettings().getIndexMetadata();
            FieldStorageResolver resolver = new FieldStorageResolver(indexMetadata);
            return fieldName -> {
                try {
                    List<FieldStorageInfo> infos = resolver.resolve(List.of(fieldName));
                    return infos.isEmpty() ? null : infos.get(0);
                } catch (IllegalStateException e) {
                    return null;
                }
            };
        } catch (Exception e) {
            logger.debug("Failed to build field storage provider for shard [{}]: {}", searchContext.indexShard().shardId(), e.getMessage());
            return null;
        }
    }
}
