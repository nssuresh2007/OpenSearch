/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.apache.lucene.util.FixedBitSet;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Java-side registry that holds per-segment bitsets for access by the FFM bridge.
 *
 * <p>When the delegation service registers a set of per-segment bitsets, this registry
 * assigns a unique {@code providerKey} that the backend executor passes across the FFM
 * boundary. The Rust-side indexed-table executor uses the provider key to request packed
 * {@code u64} words from the bitset via the existing FFM callback table.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #registerBitsets(PerSegmentBitsets)} — stores bitsets, returns provider key</li>
 *   <li>Backend executor passes provider key across FFM; Rust side calls back to retrieve
 *       bitset slices via {@link #createCollector(int, long, int, int)} and
 *       {@link #collectDocs(int, int, int, MemorySegment, int)}</li>
 *   <li>{@link #releaseCollector(int)} — removes the collector entry</li>
 *   <li>{@link #releaseProvider(int)} — removes the provider entry, allowing GC</li>
 * </ol>
 *
 * <p>FFM callback entry points: the Rust-side indexed-table executor calls back into Java
 * through the existing {@code df_register_filter_tree_callbacks} mechanism. For aggregation-time
 * providers, the callbacks route to this registry's {@link #createCollector},
 * {@link #collectDocs}, and {@link #releaseCollector} methods. The bitset is precomputed by
 * Lucene during the query phase; {@code collectDocs} returns a slice of the bitset by
 * min/max range as packed {@code u64} words in little-endian format.
 *
 * <p>Thread safety: all operations are thread-safe. Multiple concurrent requests may
 * register, create collectors, and release providers independently.
 *
 * @opensearch.internal
 */
public class BitsetProviderRegistry {

    private static final AtomicInteger KEY_GENERATOR = new AtomicInteger(0);
    private static final AtomicInteger COLLECTOR_KEY_GENERATOR = new AtomicInteger(0);

    private final Map<Integer, PerSegmentBitsets> providers = new ConcurrentHashMap<>();
    private final Map<Integer, CollectorEntry> collectors = new ConcurrentHashMap<>();

    /**
     * Registers the given per-segment bitsets and returns a unique provider key.
     *
     * @param bitsets the per-segment bitsets to register
     * @return the provider key to pass to the backend executor
     */
    public int registerBitsets(PerSegmentBitsets bitsets) {
        int key = KEY_GENERATOR.incrementAndGet();
        providers.put(key, bitsets);
        return key;
    }

    /**
     * Retrieves the bitset for a specific segment ordinal within a registered provider.
     *
     * @param providerKey   the provider key returned by {@link #registerBitsets}
     * @param segmentOrd    the segment ordinal (leaf.ord) to look up
     * @return the {@link FixedBitSet} for the segment, or {@code null} if not found
     */
    public FixedBitSet getBitsetForSegment(int providerKey, int segmentOrd) {
        PerSegmentBitsets bitsets = providers.get(providerKey);
        if (bitsets == null) {
            return null;
        }
        for (PerSegmentBitsets.BitsetEntry entry : bitsets.entries()) {
            if (entry.leaf().ord == segmentOrd) {
                if (entry.kind() == PerSegmentBitsets.BitsetKind.MATCH_ALL_LIVE) {
                    // Create a full bitset for match-all-live
                    int maxDoc = entry.leaf().reader().maxDoc();
                    FixedBitSet fullBitset = new FixedBitSet(maxDoc);
                    fullBitset.set(0, maxDoc);
                    return fullBitset;
                }
                return entry.bitset();
            }
        }
        return null;
    }

    /**
     * Returns the registered {@link PerSegmentBitsets} for the given provider key,
     * or {@code null} if no provider is registered with that key.
     *
     * @param providerKey the provider key
     * @return the per-segment bitsets, or {@code null}
     */
    public PerSegmentBitsets getProvider(int providerKey) {
        return providers.get(providerKey);
    }

    /**
     * Releases the provider associated with the given key, removing it from the registry
     * and allowing the bitsets to be garbage collected.
     *
     * @param providerKey the provider key to release
     */
    public void releaseProvider(int providerKey) {
        providers.remove(providerKey);
    }

    /**
     * Returns the number of currently registered providers. Useful for testing and monitoring.
     */
    public int activeProviderCount() {
        return providers.size();
    }

    // ── FFM callback entry points ─────────────────────────────────────────────

    /**
     * Creates a collector scoped to a specific segment (identified by writerGeneration)
     * and doc range. The collector holds a reference to the segment's precomputed
     * {@link FixedBitSet} and the requested doc range.
     *
     * <p>This is the Java-side entry point for the Rust {@code createCollector} FFM callback.
     *
     * @param providerKey      the provider key returned by {@link #registerBitsets}
     * @param writerGeneration the writer generation identifying the segment
     * @param docMin           inclusive lower bound of the doc range
     * @param docMax           exclusive upper bound of the doc range
     * @return a unique collector key {@code >= 0}, or {@code -1} if the provider or segment
     *         is not found
     */
    public int createCollector(int providerKey, long writerGeneration, int docMin, int docMax) {
        PerSegmentBitsets bitsets = providers.get(providerKey);
        if (bitsets == null) {
            return -1;
        }

        // Find the BitsetEntry matching the writerGeneration
        PerSegmentBitsets.BitsetEntry matchingEntry = null;
        for (PerSegmentBitsets.BitsetEntry entry : bitsets.entries()) {
            if (entry.writerGeneration() == writerGeneration) {
                matchingEntry = entry;
                break;
            }
        }
        if (matchingEntry == null) {
            return -1;
        }

        // Resolve the FixedBitSet: for MATCH_ALL_LIVE, fabricate a full bitset
        FixedBitSet bitset;
        if (matchingEntry.kind() == PerSegmentBitsets.BitsetKind.MATCH_ALL_LIVE) {
            int maxDoc = matchingEntry.leaf().reader().maxDoc();
            bitset = new FixedBitSet(maxDoc);
            bitset.set(0, maxDoc);
        } else {
            bitset = matchingEntry.bitset();
        }

        if (bitset == null) {
            return -1;
        }

        int collectorKey = COLLECTOR_KEY_GENERATOR.incrementAndGet();
        collectors.put(collectorKey, new CollectorEntry(bitset, docMin, docMax));
        return collectorKey;
    }

    /**
     * Reads packed {@code u64} words from the precomputed {@link FixedBitSet} for the
     * given doc range and copies them into the caller-provided {@link MemorySegment} buffer.
     *
     * <p>This is the Java-side entry point for the Rust {@code collectDocs} FFM callback.
     * The packed words are written in native (little-endian on x86) {@code u64} format,
     * matching what the Rust side expects.
     *
     * <p>Bit layout: word {@code i} contains matches for docs
     * {@code [docMin + i*64, docMin + (i+1)*64)}, LSB-first within each word.
     *
     * <p>Boundary masking: if {@code docMin} doesn't align to a 64-bit word boundary in
     * the backing array, the lower bits are masked out. If {@code docMax} doesn't align,
     * the upper bits are masked out.
     *
     * @param collectorKey the collector key returned by {@link #createCollector}
     * @param docMin       inclusive lower bound of the doc range to extract
     * @param docMax       exclusive upper bound of the doc range to extract
     * @param outBuf       the caller-provided {@link MemorySegment} to write packed words into
     * @param outWordCap   the capacity of {@code outBuf} in {@code u64} words
     * @return the number of words written, or {@code -1} on error
     */
    public int collectDocs(int collectorKey, int docMin, int docMax, MemorySegment outBuf, int outWordCap) {
        CollectorEntry entry = collectors.get(collectorKey);
        if (entry == null) {
            return -1;
        }
        if (docMax <= docMin) {
            return 0;
        }

        FixedBitSet bitset = entry.bitset;
        long[] bits = bitset.getBits();

        // Calculate the word range in the backing array that covers [docMin, docMax)
        int startWord = docMin >>> 6; // docMin / 64
        int endWord = (docMax - 1) >>> 6; // word containing (docMax - 1)

        // Number of output words needed: one word per 64 docs in [docMin, docMax)
        int span = docMax - docMin;
        int wordsNeeded = (span + 63) >>> 6;
        int wordsToWrite = Math.min(wordsNeeded, outWordCap);

        // The output is relative to docMin: output word i covers [docMin + i*64, docMin + (i+1)*64)
        // We need to shift bits from the backing array to align with docMin.
        int bitOffset = docMin & 63; // offset within the first word

        if (bitOffset == 0) {
            // Aligned case: words map directly from the backing array
            int srcLen = bits.length;
            for (int i = 0; i < wordsToWrite; i++) {
                int srcIdx = startWord + i;
                long word = (srcIdx < srcLen) ? bits[srcIdx] : 0L;

                // Mask boundary for the last output word
                if (i == wordsToWrite - 1) {
                    int bitsInLastWord = span - (i * 64);
                    if (bitsInLastWord < 64) {
                        word &= (1L << bitsInLastWord) - 1;
                    }
                }

                outBuf.setAtIndex(ValueLayout.JAVA_LONG, i, word);
            }
        } else {
            // Unaligned case: each output word is composed from two adjacent source words
            int srcLen = bits.length;
            for (int i = 0; i < wordsToWrite; i++) {
                int loIdx = startWord + i;
                int hiIdx = loIdx + 1;

                long lo = (loIdx < srcLen) ? bits[loIdx] : 0L;
                long hi = (hiIdx < srcLen) ? bits[hiIdx] : 0L;

                // Shift right to align: take upper bits of lo and lower bits of hi
                long word = (lo >>> bitOffset) | (hi << (64 - bitOffset));

                // Mask boundary for the first output word (mask out bits below docMin)
                // Not needed here because the shift already handles alignment to docMin

                // Mask boundary for the last output word
                if (i == wordsToWrite - 1) {
                    int bitsInLastWord = span - (i * 64);
                    if (bitsInLastWord < 64) {
                        word &= (1L << bitsInLastWord) - 1;
                    }
                }

                outBuf.setAtIndex(ValueLayout.JAVA_LONG, i, word);
            }
        }

        return wordsToWrite;
    }

    /**
     * Releases the collector entry associated with the given key, removing it from the
     * internal map and allowing the referenced bitset to be garbage collected (if no other
     * collectors reference it).
     *
     * <p>This is the Java-side entry point for the Rust {@code releaseCollector} FFM callback.
     *
     * @param collectorKey the collector key to release
     */
    public void releaseCollector(int collectorKey) {
        collectors.remove(collectorKey);
    }

    /**
     * Returns the number of currently active collectors. Useful for testing and monitoring.
     */
    public int activeCollectorCount() {
        return collectors.size();
    }

    /**
     * Internal entry holding the state for a single collector: the precomputed bitset
     * and the doc range it was created for.
     */
    private record CollectorEntry(FixedBitSet bitset, int docMin, int docMax) {
    }
}
