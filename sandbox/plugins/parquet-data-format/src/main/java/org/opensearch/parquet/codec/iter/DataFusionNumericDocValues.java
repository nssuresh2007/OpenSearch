/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.NumericDocValues;
import org.opensearch.parquet.bridge.DataFusionColumnReader;
import org.opensearch.parquet.codec.cache.PageCache;

import java.io.IOException;

/**
 * {@link NumericDocValues} over a single-valued Parquet primitive column, served from
 * DataFusion-decoded Arrow batches ({@code parquet.docvalues.decode_path=datafusion}).
 *
 * <p>Mirrors {@link ParquetNumericDocValues}: a hit is a presence bit-test plus a {@code long[]}
 * index lookup against the reader's buffered batch (no FFM crossing); a miss pulls the next
 * covering batch via {@link DataFusionColumnReader#loadBatchContaining}. Forward-only, matching the
 * Lucene doc-values contract and the forward-only DataFusion stream underneath.
 */
public final class DataFusionNumericDocValues extends NumericDocValues {

    private final DataFusionColumnReader reader;
    private final int maxDoc;

    private int doc = -1;
    private long currentValue;
    private boolean currentPresent;

    public DataFusionNumericDocValues(DataFusionColumnReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            return false;
        }
        doc = target;
        PageCache cache = reader.cache();
        if (cache != null && target >= cache.firstRow && target <= cache.lastRow) {
            // In-range hit — served from the buffered batch, no FFM crossing.
        } else {
            reader.loadBatchContaining(target);
            cache = reader.cache();
            if (cache == null) {
                currentPresent = false;
                currentValue = 0L;
                return false;
            }
        }
        currentPresent = cache.isPresent(target);
        currentValue = currentPresent ? cache.valueAt(target) : 0L;
        return currentPresent;
    }

    @Override
    public long longValue() {
        return currentValue;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                doc = d;
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
