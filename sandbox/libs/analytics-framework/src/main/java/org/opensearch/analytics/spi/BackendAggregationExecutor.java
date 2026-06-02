/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.internal.SearchContext;

/**
 * SPI surface for per-segment aggregation execution on a columnar backend.
 *
 * <p>The delegation framework on the data node invokes this interface once per Lucene segment
 * that contributes matching documents to the request. The implementation receives the eligible
 * aggregation subtree, the segment's matching docId bitset (produced by Lucene's standard
 * query phase), and the search context, and returns partial-aggregation state shaped as an
 * {@link InternalAggregation} subclass that the coordinator's existing reduce path can combine.
 *
 * <p>DataFusion provides the v1 implementation; future backends (tantivy, vectors, other
 * columnar engines) register their own implementations through
 * {@code AnalyticsSearchBackendPlugin.aggregationExecutor()}.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #executeOnSegment} returns an {@code InternalAggregation} directly — each
 *       backend is responsible for converting its native output to OpenSearch's
 *       {@code InternalAggregation} shape.</li>
 *   <li>The {@code FixedBitSet} argument carries the matching docs after Lucene query +
 *       post-filter + {@code liveDocs}. For the match-all-live case the bitset is the
 *       segment's {@code liveDocs} (or a full-set sentinel).</li>
 *   <li>Implementations MUST release every native resource (Arrow buffers, FFM handles,
 *       sessions) before returning, regardless of success or failure.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public interface BackendAggregationExecutor {

    /**
     * Stable backend identifier, matching {@link AnalyticsSearchBackendPlugin#name()}.
     */
    String backendId();

    /**
     * Whether this backend can evaluate the given aggregation against the given field's
     * storage info. Pure function — no side effects. Used by the eligibility classifier
     * as a final fitness check after {@code CapabilityRegistry} has narrowed the candidate set.
     *
     * @param agg   the aggregation builder (carries type, field references, parameters)
     * @param field the field's storage metadata (doc-value formats, field type, etc.)
     * @return {@code true} if this backend can evaluate the aggregation on this field
     */
    boolean canEvaluate(
        org.opensearch.search.aggregations.AggregationBuilder agg,
        FieldStorageInfo field
    );

    /**
     * Execute the eligible aggregation subtree on a single Lucene segment, restricted to
     * the docs in the bitset. Returns per-segment partial-aggregation state shaped as an
     * {@code InternalAggregation} subclass that the coordinator's existing reduce path
     * can combine.
     *
     * <p>The implementation may translate the aggregation tree into its native plan format
     * (e.g. Substrait for DataFusion), apply the bitset as a row filter over the segment's
     * columnar file, execute the plan, and convert the result back to an
     * {@code InternalAggregation}.
     *
     * @param eligibleSubtree the aggregation subtree classified as eligible (may contain
     *                        nested sub-aggregations)
     * @param leaf            the Lucene segment to execute against
     * @param matchingDocs    the set of matching docIds within this segment (bit {@code i}
     *                        is set iff docId {@code i} is in the matching set)
     * @param ctx             the search context (provides access to MapperService, search
     *                        task for cancellation polling, circuit breaker for memory
     *                        accounting)
     * @return per-segment partial aggregation state
     * @throws BackendExecutionException if the backend encounters an error during execution
     */
    InternalAggregation executeOnSegment(
        AggregatorFactories eligibleSubtree,
        LeafReaderContext leaf,
        FixedBitSet matchingDocs,
        SearchContext ctx
    ) throws BackendExecutionException;
}
