/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * Optional interface that a {@link BackendAggregationExecutor} may implement to receive
 * per-segment execution context (provider key, writer generation, original aggregation)
 * before each {@link BackendAggregationExecutor#executeOnSegment} invocation.
 *
 * <p>The delegation framework calls {@link #setSegmentContext(int, long, AggregationBuilder)}
 * immediately before each {@code executeOnSegment} call so the executor has the FFM-bridge
 * parameters it needs (bitset provider key for the Rust-side row filter, writer generation
 * identifying the target Parquet segment) and the original aggregation builder for result
 * conversion.
 *
 * <p>This interface avoids changing the {@link BackendAggregationExecutor} SPI contract while
 * allowing backends that need additional per-segment metadata to receive it through a
 * well-defined mechanism.
 *
 * @opensearch.internal
 */
public interface SegmentExecutionContext {

    /**
     * Sets the per-segment execution context before an {@code executeOnSegment} call.
     *
     * @param providerKey      the bitset provider key registered in {@code BitsetProviderRegistry},
     *                         identifying the pre-computed matching-doc bitsets for FFM access
     * @param writerGeneration the composite-engine writer generation identifying the target
     *                         Parquet segment file
     * @param originalAgg      the original {@link AggregationBuilder} for the aggregation being
     *                         executed — needed by the backend for Substrait plan construction
     *                         and result conversion
     */
    void setSegmentContext(int providerKey, long writerGeneration, AggregationBuilder originalAgg);
}
