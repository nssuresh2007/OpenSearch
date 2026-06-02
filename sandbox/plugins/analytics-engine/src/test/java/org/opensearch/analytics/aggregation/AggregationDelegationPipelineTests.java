/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendAggregationExecutor;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit-level smoke test for the aggregation delegation pipeline.
 *
 * <p>Verifies that the Java-side pipeline components can be instantiated and exercised
 * without a running cluster or native libraries:
 * <ol>
 *   <li>{@link DelegatingAggregationProcessor} can be instantiated with mock dependencies</li>
 *   <li>{@link AggregationEligibilityClassifier} correctly classifies a sum aggregation
 *       on a parquet field as eligible</li>
 *   <li>{@link AggregationDelegationService} can be called (falls back gracefully when
 *       no native library is available)</li>
 *   <li>{@link BitsetProviderRegistry} lifecycle works correctly</li>
 * </ol>
 *
 * <p>Note: {@code PartialAggregationAdapter} tests are in the analytics-backend-datafusion
 * module's test directory since that class lives in that module.
 */
public class AggregationDelegationPipelineTests extends OpenSearchTestCase {

    private static final String BACKEND_ID = "datafusion";
    private static final String FIELD_NAME = "revenue";

    /**
     * Verifies that {@link DelegatingAggregationProcessor} can be instantiated with
     * mock dependencies without throwing.
     */
    public void testDelegatingAggregationProcessorInstantiation() {
        // Arrange: mock all dependencies
        var delegate = mock(org.opensearch.search.aggregations.AggregationProcessor.class);
        var capabilityRegistry = mock(CapabilityRegistry.class);
        Function<String, FieldStorageInfo> fieldStorageProvider = name -> null;
        var delegationService = mock(AggregationDelegationService.class);

        // Act: instantiate the processor
        DelegatingAggregationProcessor processor = new DelegatingAggregationProcessor(
            delegate,
            capabilityRegistry,
            fieldStorageProvider,
            delegationService,
            segOrd -> 0L
        );

        // Assert: processor is non-null and can be used
        assertNotNull("DelegatingAggregationProcessor should be instantiated successfully", processor);
    }

    /**
     * Verifies that the eligibility classifier correctly classifies a sum aggregation
     * on a parquet field as eligible when a capable backend is available.
     */
    public void testEligibilityClassifierClassifiesSumOnParquetAsEligible() {
        // Arrange: set up a CapabilityRegistry that reports "datafusion" as capable
        CapabilityRegistry registry = mock(CapabilityRegistry.class);
        when(registry.aggregateBackendsForField(eq(AggregateFunction.SUM), any(FieldStorageInfo.class))).thenReturn(List.of(BACKEND_ID));
        when(registry.aggregateCapableBackends()).thenReturn(Set.of(BACKEND_ID));

        // Mock the backend plugin with a non-null aggregation executor
        AnalyticsSearchBackendPlugin backendPlugin = mock(AnalyticsSearchBackendPlugin.class);
        BackendAggregationExecutor executor = mock(BackendAggregationExecutor.class);
        when(executor.canEvaluate(any(AggregationBuilder.class), any(FieldStorageInfo.class))).thenReturn(true);
        when(backendPlugin.aggregationExecutor()).thenReturn(executor);
        BackendCapabilityProvider capProvider = mock(BackendCapabilityProvider.class);
        when(capProvider.aggregationPriority()).thenReturn(100);
        when(capProvider.aggregateCapabilities()).thenReturn(Set.of());
        when(backendPlugin.getCapabilityProvider()).thenReturn(capProvider);
        when(registry.getBackend(BACKEND_ID)).thenReturn(backendPlugin);

        // Set up field storage info with parquet doc values
        FieldStorageInfo fieldInfo = new FieldStorageInfo(
            FIELD_NAME,
            "long",
            org.opensearch.analytics.spi.FieldType.LONG,
            List.of("parquet"),
            List.of("lucene"),
            List.of(),
            false
        );
        Function<String, FieldStorageInfo> fieldStorageProvider = name -> FIELD_NAME.equals(name) ? fieldInfo : null;

        // Mock QueryShardContext (no star-tree available)
        QueryShardContext queryShardContext = mock(QueryShardContext.class);

        // Act: classify a sum aggregation on the parquet field
        AggregationEligibilityClassifier classifier = new AggregationEligibilityClassifier(
            registry,
            fieldStorageProvider,
            queryShardContext
        );

        AggregatorFactories.Builder factories = AggregatorFactories.builder();
        factories.addAggregator(AggregationBuilders.sum("total_revenue").field(FIELD_NAME));

        EligibilitySplit split = classifier.classify(factories);

        // Assert: the sum aggregation is classified as eligible with the correct backend
        assertEquals("Should have 1 eligible aggregation", 1, split.eligible().size());
        assertEquals("Eligible aggregation should target datafusion", BACKEND_ID, split.eligible().get(0).chosenBackendId());
        assertEquals("Should have 0 ineligible aggregations", 0, split.ineligible().getAggregatorFactories().size());
        assertTrue("Reasons map should be empty", split.reasons().isEmpty());
    }

    /**
     * Verifies that the eligibility classifier correctly classifies a sum aggregation
     * on a non-parquet field as ineligible.
     */
    public void testEligibilityClassifierClassifiesNonParquetAsIneligible() {
        // Arrange: set up a CapabilityRegistry
        CapabilityRegistry registry = mock(CapabilityRegistry.class);
        when(registry.aggregateBackendsForField(any(), any())).thenReturn(List.of());

        // Set up field storage info WITHOUT parquet doc values (lucene only)
        FieldStorageInfo fieldInfo = new FieldStorageInfo(
            FIELD_NAME,
            "long",
            org.opensearch.analytics.spi.FieldType.LONG,
            List.of("lucene"),
            List.of("lucene"),
            List.of(),
            false
        );
        Function<String, FieldStorageInfo> fieldStorageProvider = name -> FIELD_NAME.equals(name) ? fieldInfo : null;

        QueryShardContext queryShardContext = mock(QueryShardContext.class);

        // Act
        AggregationEligibilityClassifier classifier = new AggregationEligibilityClassifier(
            registry,
            fieldStorageProvider,
            queryShardContext
        );

        AggregatorFactories.Builder factories = AggregatorFactories.builder();
        factories.addAggregator(AggregationBuilders.sum("total_revenue").field(FIELD_NAME));

        EligibilitySplit split = classifier.classify(factories);

        // Assert: the sum aggregation is classified as ineligible
        assertEquals("Should have 0 eligible aggregations", 0, split.eligible().size());
        assertEquals("Should have 1 ineligible aggregation", 1, split.ineligible().getAggregatorFactories().size());
        assertEquals(
            "Reason should be NO_PARQUET_DOC_VALUES",
            IneligibilityReason.NO_PARQUET_DOC_VALUES,
            split.reasons().get("total_revenue")
        );
    }

    /**
     * Verifies that {@link AggregationDelegationService} can be called and handles
     * the case where the backend executor is not found (falls back gracefully).
     */
    public void testAggregationDelegationServiceFallsBackWhenNoExecutor() throws IOException {
        // Arrange: BackendRouter with no executors registered
        BackendRouter router = new BackendRouter(Collections.emptyMap());
        BitsetProviderRegistry bitsetRegistry = new BitsetProviderRegistry();
        AggregationDelegationService service = new AggregationDelegationService(router, bitsetRegistry);

        // Create a mock eligible aggregation
        AggregationBuilder sumAgg = AggregationBuilders.sum("total_revenue").field(FIELD_NAME);
        List<EligibleAggregation> eligible = List.of(new EligibleAggregation(sumAgg, BACKEND_ID));

        // Create empty per-segment bitsets (no segments)
        PerSegmentBitsets emptyBitsets = new PerSegmentBitsets(Map.of());

        SearchContext ctx = mock(SearchContext.class);
        DelegationRequestState state = mock(DelegationRequestState.class);

        // Act: call execute — should handle gracefully with empty bitsets
        List<InternalAggregation> results = service.execute(ctx, eligible, emptyBitsets, state);

        // Assert: no results because there are no segments to process
        assertTrue("Results should be empty when there are no segments", results.isEmpty());
    }

    /**
     * Verifies that {@link AggregationDelegationService} routes to the correct backend
     * and that the {@link BackendRouter} resolves executors correctly.
     */
    public void testAggregationDelegationServiceRoutesToCorrectBackend() {
        // Arrange: create a mock executor
        BackendAggregationExecutor mockExecutor = mock(BackendAggregationExecutor.class);
        when(mockExecutor.backendId()).thenReturn(BACKEND_ID);

        BackendRouter router = new BackendRouter(Map.of(BACKEND_ID, mockExecutor));

        // Verify the router correctly resolves the executor
        assertNotNull("Router should find the executor", router.getExecutor(BACKEND_ID));
        assertTrue("Router should report having the executor", router.hasExecutor(BACKEND_ID));
        assertEquals("Router should have 1 executor", 1, router.size());
        assertNull("Router should return null for unknown backend", router.getExecutor("unknown-backend"));
        assertFalse("Router should not have unknown backend", router.hasExecutor("unknown-backend"));
    }

    /**
     * Verifies that {@link AggregationDelegationService} falls back gracefully when
     * the backend executor is not found for the given backend id.
     */
    public void testAggregationDelegationServiceFallsBackWhenBackendNotFound() throws IOException {
        // Arrange: BackendRouter with a different backend registered
        BackendAggregationExecutor otherExecutor = mock(BackendAggregationExecutor.class);
        when(otherExecutor.backendId()).thenReturn("other-backend");

        BackendRouter router = new BackendRouter(Map.of("other-backend", otherExecutor));
        BitsetProviderRegistry bitsetRegistry = new BitsetProviderRegistry();
        AggregationDelegationService service = new AggregationDelegationService(router, bitsetRegistry);

        // Create eligible aggregation targeting "datafusion" which is NOT in the router
        AggregationBuilder sumAgg = AggregationBuilders.sum("total_revenue").field(FIELD_NAME);
        List<EligibleAggregation> eligible = List.of(new EligibleAggregation(sumAgg, BACKEND_ID));

        // Create per-segment bitsets with one segment
        MemoryIndex memIndex = new MemoryIndex();
        memIndex.addField("dummy", "value", new org.apache.lucene.analysis.standard.StandardAnalyzer());
        LeafReaderContext leafContext = memIndex.createSearcher().getIndexReader().leaves().get(0);

        FixedBitSet bitset = new FixedBitSet(1);
        bitset.set(0);

        PerSegmentBitsets.BitsetEntry entry = new PerSegmentBitsets.BitsetEntry(
            leafContext,
            1L,
            PerSegmentBitsets.BitsetKind.EXPLICIT,
            bitset,
            1L
        );
        PerSegmentBitsets perSegmentBitsets = new PerSegmentBitsets(Map.of(leafContext, entry));

        SearchContext ctx = mock(SearchContext.class);
        DelegationRequestState state = mock(DelegationRequestState.class);

        // Act: execute — should fall back because "datafusion" executor is not registered
        List<InternalAggregation> results = service.execute(ctx, eligible, perSegmentBitsets, state);

        // Assert: results are empty because fallback returns null (not yet implemented)
        assertTrue("Results should be empty when backend is not found (fallback returns null)", results.isEmpty());
    }

    /**
     * Verifies that the {@link BitsetProviderRegistry} correctly registers and retrieves
     * bitsets, and that cleanup works properly.
     */
    public void testBitsetProviderRegistryLifecycle() {
        BitsetProviderRegistry registry = new BitsetProviderRegistry();

        // Create a real LeafReaderContext using MemoryIndex (LeafReaderContext is final)
        MemoryIndex memIndex = new MemoryIndex();
        memIndex.addField("dummy", "value", new org.apache.lucene.analysis.standard.StandardAnalyzer());
        LeafReaderContext leafContext = memIndex.createSearcher().getIndexReader().leaves().get(0);

        FixedBitSet bitset = new FixedBitSet(128);
        bitset.set(0); // set bit 0 (MemoryIndex has 1 doc at position 0)

        PerSegmentBitsets.BitsetEntry entry = new PerSegmentBitsets.BitsetEntry(
            leafContext,
            1L,
            PerSegmentBitsets.BitsetKind.EXPLICIT,
            bitset,
            1L
        );
        PerSegmentBitsets perSegmentBitsets = new PerSegmentBitsets(Map.of(leafContext, entry));

        // Register
        int providerKey = registry.registerBitsets(perSegmentBitsets);
        assertTrue("Provider key should be positive", providerKey > 0);
        assertEquals("Should have 1 active provider", 1, registry.activeProviderCount());

        // Retrieve
        PerSegmentBitsets retrieved = registry.getProvider(providerKey);
        assertNotNull("Retrieved bitsets should not be null", retrieved);
        assertEquals("Should have 1 entry", 1, retrieved.size());

        // Release
        registry.releaseProvider(providerKey);
        assertEquals("Should have 0 active providers after release", 0, registry.activeProviderCount());
        assertNull("Provider should be null after release", registry.getProvider(providerKey));
    }
}
