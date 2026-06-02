/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.FixedBitSet;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Container holding per-segment bitset entries produced by the {@code DocIdBitsetCollectorManager}.
 * Each entry is keyed by its {@link LeafReaderContext} and carries the matching-doc bitset
 * (or a match-all-live sentinel) along with metadata needed to bridge the bitset across the FFM
 * boundary to the backend executor.
 *
 * @opensearch.internal
 */
public final class PerSegmentBitsets {

    /**
     * Kind of bitset entry: either an explicit {@link FixedBitSet} was collected, or the segment
     * matches all live documents and no per-doc bitset is needed.
     */
    public enum BitsetKind {
        /** A real {@link FixedBitSet} was collected during the query phase. */
        EXPLICIT,
        /** Sentinel: the outer query is MatchAllDocsQuery and the segment has no deletions. */
        MATCH_ALL_LIVE
    }

    /**
     * A single segment's bitset entry.
     *
     * @param leaf             the Lucene leaf (segment) context this entry belongs to
     * @param writerGeneration the composite-engine writer generation used to key per-segment
     *                         bitsets across the FFM bridge
     * @param kind             whether this is an explicit bitset or a match-all-live sentinel
     * @param bitset           the matching-doc bitset; {@code null} when {@code kind} is
     *                         {@link BitsetKind#MATCH_ALL_LIVE}
     * @param cardinality      number of matching documents in this segment
     */
    public record BitsetEntry(LeafReaderContext leaf, long writerGeneration, BitsetKind kind, FixedBitSet bitset, long cardinality) {
    }

    private final Map<LeafReaderContext, BitsetEntry> entries;

    /**
     * Creates a new {@code PerSegmentBitsets} from the given entries map.
     *
     * @param entries map from leaf context to bitset entry; iteration order is preserved
     */
    public PerSegmentBitsets(Map<LeafReaderContext, BitsetEntry> entries) {
        this.entries = Collections.unmodifiableMap(new LinkedHashMap<>(entries));
    }

    /**
     * Returns the bitset entry for the given segment, or {@code null} if no entry exists.
     */
    public BitsetEntry get(LeafReaderContext leaf) {
        return entries.get(leaf);
    }

    /**
     * Returns all bitset entries in segment order.
     */
    public Collection<BitsetEntry> entries() {
        return entries.values();
    }

    /**
     * Returns the number of segments with bitset entries.
     */
    public int size() {
        return entries.size();
    }

    /**
     * Returns {@code true} if there are no segment entries (all segments were empty).
     */
    public boolean isEmpty() {
        return entries.isEmpty();
    }
}
