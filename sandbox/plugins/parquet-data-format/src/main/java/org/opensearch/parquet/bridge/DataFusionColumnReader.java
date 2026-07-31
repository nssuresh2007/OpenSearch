/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.parquet.codec.cache.PageCache;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

/**
 * DataFusion-backed equivalent of {@link ParquetColumnReader} for the
 * {@code parquet.docvalues.decode_path=datafusion} path.
 *
 * <p>Instead of the hand-written page decoder, this reader drives a forward-only single-column
 * DataFusion stream (opened via {@link RustBridge#dfOpenIter}). Each {@link #loadBatchContaining}
 * pulls the Arrow batch covering a row and materializes it into a {@link PageCache} — the same
 * in-memory structure the native path uses — so iterators serve subsequent in-range rows from
 * memory with no further FFM crossing. The decode-once-per-chunk amortization is preserved at
 * Arrow-batch granularity.
 *
 * <p>Primitive numeric columns only (values are raw {@code i64} words + a presence bitset, layout
 * identical to {@code parquet_decode_page_at_row}). Single-threaded; {@link #close()} is idempotent.
 */
public final class DataFusionColumnReader implements Closeable {

    private static final long CLOSED_HANDLE = -1L;

    /** Initial batch-size guess (rows) for the out-buffers; grown on overflow. */
    private static final int INITIAL_ROWS = 8192;

    private final BufferPool bufferPool;
    private final Path file;
    private final String column;

    private long handle;
    private PageCache cache;

    private DataFusionColumnReader(long handle, Path file, String column, BufferPool bufferPool) {
        this.handle = handle;
        this.file = file;
        this.column = column;
        this.bufferPool = bufferPool;
    }

    /** Opens a DataFusion doc-values stream for {@code column} in {@code file}. */
    public static DataFusionColumnReader open(Path file, String column, BufferPool pool) throws IOException {
        long h = RustBridge.dfOpenIter(file.toString(), column);
        return new DataFusionColumnReader(h, file, column, pool);
    }

    /** The currently buffered batch, or {@code null} if none is loaded yet. */
    public PageCache cache() {
        return cache;
    }

    /**
     * Loads the Arrow batch containing global {@code row} into {@link #cache}. Follows the same
     * grow-and-retry overflow protocol as the native page decode. Because the DataFusion stream is
     * forward-only, {@code row} must be non-decreasing across calls on this reader.
     */
    public void loadBatchContaining(long row) throws IOException {
        ensureOpen();
        MemorySegment firstRowOut = bufferPool.longOut("dfFirstRow");
        MemorySegment lastRowOut = bufferPool.longOut("dfLastRow");
        MemorySegment valueLenOut = bufferPool.longOut("dfValueLen");

        int rows = INITIAL_ROWS;
        int presenceWords = (rows + 63) >>> 6;
        long valueCap = (long) rows * ValueLayout.JAVA_LONG.byteSize();
        MemorySegment valueBuf = bufferPool.bytes("dfValue", valueCap);
        MemorySegment presence = bufferPool.longs("dfPresence", presenceWords);

        long rc = RustBridge.dfNextBatch(handle, row, firstRowOut, lastRowOut, valueBuf, valueCap, valueLenOut, presence, presenceWords);
        if (rc == RustBridge.RC_OVERFLOW) {
            long fr = firstRowOut.get(ValueLayout.JAVA_LONG, 0);
            long lr = lastRowOut.get(ValueLayout.JAVA_LONG, 0);
            int actualRows = (int) (lr - fr + 1);
            int actualPresenceWords = (actualRows + 63) >>> 6;
            long requiredValueBytes = valueLenOut.get(ValueLayout.JAVA_LONG, 0);

            valueCap = Math.max(requiredValueBytes, 1);
            valueBuf = bufferPool.bytes("dfValue", valueCap);
            presence = bufferPool.longs("dfPresence", actualPresenceWords);
            presenceWords = actualPresenceWords;

            rc = RustBridge.dfNextBatch(handle, row, firstRowOut, lastRowOut, valueBuf, valueCap, valueLenOut, presence, presenceWords);
            if (rc == RustBridge.RC_OVERFLOW) {
                throw new IOException("dfNextBatch: overflow persisted after retry at row " + row + " (" + file + "/" + column + ")");
            }
        }
        if (rc == RustBridge.RC_EOF) {
            throw new IOException(
                "dfNextBatch: stream exhausted before row " + row + " (" + file + "/" + column + "); row/docId mismatch?"
            );
        }

        long firstRow = firstRowOut.get(ValueLayout.JAVA_LONG, 0);
        long lastRow = lastRowOut.get(ValueLayout.JAVA_LONG, 0);
        int batchRows = (int) (lastRow - firstRow + 1);

        PageCache pc = new PageCache();
        pc.firstRow = firstRow;
        pc.lastRow = lastRow;
        pc.values = valueBuf.asSlice(0, (long) batchRows * ValueLayout.JAVA_LONG.byteSize()).toArray(ValueLayout.JAVA_LONG);
        pc.presenceBits = presence.asSlice(0, (long) ((batchRows + 63) >>> 6) * ValueLayout.JAVA_LONG.byteSize())
            .toArray(ValueLayout.JAVA_LONG);
        cache = pc;
    }

    private void ensureOpen() {
        if (handle == CLOSED_HANDLE) {
            throw new IllegalStateException("DataFusionColumnReader is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (handle == CLOSED_HANDLE) {
            return;
        }
        long h = handle;
        handle = CLOSED_HANDLE;
        cache = null;
        RustBridge.dfCloseIter(h);
    }
}
