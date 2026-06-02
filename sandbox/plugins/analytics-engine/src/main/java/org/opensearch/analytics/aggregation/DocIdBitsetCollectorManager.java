/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongUnaryOperator;

/**
 * A Lucene {@link CollectorManager} that records each segment's matching docIds into a
 * {@link FixedBitSet} during the normal query-phase collection pass.
 *
 * <p>This collector is registered alongside the standard Lucene aggregators so that it
 * observes every matching doc in the same scorer pass — no second pass over the segment
 * is needed. For {@code ConcurrentQueryPhaseSearcher}'s slice-fanout, one collector is
 * created per slice; per-slice partial bitsets are merged in {@link #reduce(Collection)}
 * into one bitset per {@link LeafReaderContext}.
 *
 * <p><b>Match-all-live fast path:</b></p>
 * When the outer query is {@link MatchAllDocsQuery} and the segment has no deletions,
 * the collector fabricates a sentinel {@link PerSegmentBitsets.BitsetEntry} with
 * {@link PerSegmentBitsets.BitsetKind#MATCH_ALL_LIVE} without per-doc {@code set()} calls.
 * This lets the downstream backend executor elide bitset checks entirely.
 *
 * @opensearch.internal
 */
public class DocIdBitsetCollectorManager implements CollectorManager<DocIdBitsetCollectorManager.DocIdBitsetCollector, PerSegmentBitsets> {

    private final Query outerQuery;
    private final LongUnaryOperator writerGenerationLookup;

    /**
     * Creates a new collector manager.
     *
     * @param outerQuery              the top-level query being executed; used for the match-all-live
     *                                fast-path detection
     * @param writerGenerationLookup  maps a segment ordinal (leaf.ord) to the composite-engine
     *                                writer generation for that segment; used to key bitsets across
     *                                the FFM bridge
     */
    public DocIdBitsetCollectorManager(Query outerQuery, LongUnaryOperator writerGenerationLookup) {
        this.outerQuery = outerQuery;
        this.writerGenerationLookup = writerGenerationLookup;
    }

    @Override
    public DocIdBitsetCollector newCollector() throws IOException {
        return new DocIdBitsetCollector(outerQuery, writerGenerationLookup);
    }

    @Override
    public PerSegmentBitsets reduce(Collection<DocIdBitsetCollector> collectors) throws IOException {
        Map<LeafReaderContext, PerSegmentBitsets.BitsetEntry> merged = new LinkedHashMap<>();

        for (DocIdBitsetCollector collector : collectors) {
            for (Map.Entry<LeafReaderContext, PerSegmentBitsets.BitsetEntry> entry : collector.getEntries().entrySet()) {
                LeafReaderContext leaf = entry.getKey();
                PerSegmentBitsets.BitsetEntry incoming = entry.getValue();

                PerSegmentBitsets.BitsetEntry existing = merged.get(leaf);
                if (existing == null) {
                    merged.put(leaf, incoming);
                } else {
                    // Merge: if either is MATCH_ALL_LIVE, the merged result is MATCH_ALL_LIVE
                    if (existing.kind() == PerSegmentBitsets.BitsetKind.MATCH_ALL_LIVE
                        || incoming.kind() == PerSegmentBitsets.BitsetKind.MATCH_ALL_LIVE) {
                        // Keep the MATCH_ALL_LIVE entry (already in merged or replace with incoming)
                        if (existing.kind() != PerSegmentBitsets.BitsetKind.MATCH_ALL_LIVE) {
                            merged.put(leaf, incoming);
                        }
                    } else {
                        // Both are EXPLICIT — OR the bitsets together
                        FixedBitSet mergedBits = existing.bitset();
                        mergedBits.or(incoming.bitset());
                        long mergedCardinality = mergedBits.cardinality();
                        merged.put(
                            leaf,
                            new PerSegmentBitsets.BitsetEntry(
                                leaf,
                                existing.writerGeneration(),
                                PerSegmentBitsets.BitsetKind.EXPLICIT,
                                mergedBits,
                                mergedCardinality
                            )
                        );
                    }
                }
            }
        }

        return new PerSegmentBitsets(merged);
    }

    /**
     * A {@link Collector} that builds per-segment {@link FixedBitSet}s from matching docIds.
     * Each call to {@link #getLeafCollector(LeafReaderContext)} either creates a real bitset
     * collector or, for the match-all-live fast path, fabricates a sentinel entry immediately.
     *
     * @opensearch.internal
     */
    public static class DocIdBitsetCollector implements Collector {

        private final Query outerQuery;
        private final LongUnaryOperator writerGenerationLookup;
        private final Map<LeafReaderContext, PerSegmentBitsets.BitsetEntry> entries = new LinkedHashMap<>();
        private final List<LeafBitsetCollector> leafCollectors = new ArrayList<>();

        DocIdBitsetCollector(Query outerQuery, LongUnaryOperator writerGenerationLookup) {
            this.outerQuery = outerQuery;
            this.writerGenerationLookup = writerGenerationLookup;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            long writerGeneration = writerGenerationLookup.applyAsLong(context.ord);

            // Match-all-live fast path: MatchAllDocsQuery + no deletions
            if (outerQuery instanceof MatchAllDocsQuery && context.reader().hasDeletions() == false) {
                long cardinality = context.reader().maxDoc();
                entries.put(
                    context,
                    new PerSegmentBitsets.BitsetEntry(
                        context,
                        writerGeneration,
                        PerSegmentBitsets.BitsetKind.MATCH_ALL_LIVE,
                        null,
                        cardinality
                    )
                );
                // Return a no-op leaf collector — Lucene still calls collect() but we don't need it
                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) throws IOException {
                        // no-op
                    }

                    @Override
                    public void collect(int doc) throws IOException {
                        // no-op: match-all-live sentinel already fabricated
                    }
                };
            }

            // Normal path: create a FixedBitSet and collect doc ids
            int maxDoc = context.reader().maxDoc();
            FixedBitSet bitset = new FixedBitSet(maxDoc);
            LeafBitsetCollector leafCollector = new LeafBitsetCollector(bitset, context, writerGeneration);
            leafCollectors.add(leafCollector);
            return leafCollector;
        }

        @Override
        public ScoreMode scoreMode() {
            // We only need doc ids, not scores
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        /**
         * Returns the collected per-segment bitset entries. Must be called after collection
         * is complete (i.e. after the searcher has finished the query phase).
         */
        Map<LeafReaderContext, PerSegmentBitsets.BitsetEntry> getEntries() {
            // Finalize any leaf collectors that haven't been finalized yet
            for (LeafBitsetCollector leafCollector : leafCollectors) {
                if (entries.containsKey(leafCollector.context) == false) {
                    long cardinality = leafCollector.bitset.cardinality();
                    entries.put(
                        leafCollector.context,
                        new PerSegmentBitsets.BitsetEntry(
                            leafCollector.context,
                            leafCollector.writerGeneration,
                            PerSegmentBitsets.BitsetKind.EXPLICIT,
                            leafCollector.bitset,
                            cardinality
                        )
                    );
                }
            }
            return entries;
        }
    }

    /**
     * A {@link LeafCollector} that sets bits in a {@link FixedBitSet} for each collected doc.
     *
     * @opensearch.internal
     */
    private static class LeafBitsetCollector implements LeafCollector {

        final FixedBitSet bitset;
        final LeafReaderContext context;
        final long writerGeneration;

        LeafBitsetCollector(FixedBitSet bitset, LeafReaderContext context, long writerGeneration) {
            this.bitset = bitset;
            this.context = context;
            this.writerGeneration = writerGeneration;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            // no-op: we don't need scores
        }

        @Override
        public void collect(int doc) throws IOException {
            bitset.set(doc);
        }
    }
}
