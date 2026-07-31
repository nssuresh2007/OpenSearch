/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! DataFusion-backed doc-values decode path (`parquet.docvalues.decode_path=datafusion`).
//!
//! Instead of the hand-written `parquet_decode_page_at_row` decoder, this module serves Lucene
//! doc-values reads from Arrow batches decoded by DataFusion's `ParquetSource`, and — crucially —
//! decodes **only the row group / pages containing the requested row** rather than scanning the
//! whole column. The Java adapter (`DataFusionColumnReader` / `DataFusionNumericDocValues`) buffers
//! the current batch and serves in-range `advanceExact(docId)` from it, so decode-once-per-chunk
//! amortization is preserved at Arrow-batch granularity.
//!
//! # Seek model (spec Task 3.2 + 3.3)
//! - Per iterator we hold the file's `Arc<ParquetMetaData>` and a prefix sum of per-row-group row
//!   counts, so a global `target_row` maps to `(rg_index, offset_in_rg)` by binary search.
//! - On a miss we (re)build a single-row-group stream via
//!   [`crate::df_row_selection::create_row_selection_stream`] with a positional `RowSelection`
//!   (`skip(offset_in_rg) select(rest_of_rg)`) — the reader then fetches only the pages from the
//!   target's page onward. The stream is kept alive for forward reads within the same row group;
//!   it is rebuilt only when the cursor crosses a row-group boundary.
//! - Decoded values are written as native-endian `i64` words + an LSB-first presence bitset,
//!   byte-identical to `parquet_decode_page_at_row`, so the Java read side is reused verbatim.
//!
//! Primitives only (integer/date/timestamp/boolean/float → `i64` words). BYTE_ARRAY/keyword is not
//! handled here yet (Task 5.3). Sharing DataFusion's liquid cache instance is folded in with the
//! `df-parquet-access` extraction (Task 3.1/3.4); this path currently uses a plain `ParquetSource`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tokio::runtime::Runtime;

use crate::df_row_selection::{
    create_row_selection_stream, load_parquet_metadata, ReadIoStats, RowGroupStreamConfig,
};
use crate::ffm::{RC_OK, RC_OVERFLOW};

/// EOF sentinel: the covering row group's stream ended before `target_row` was reached. For a
/// well-formed `row == docId` segment with `target_row < num_rows` this should never happen.
const RC_EOF: i64 = 2;

/// Dedicated single-threaded runtime driving DataFusion's async work from the synchronous FFM path
/// (mirrors `liquid_page_cache`'s runtime pattern).
static RT: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("df_docvalues: failed to build tokio runtime")
    })
}

/// The live row-group stream for the current position within an iterator.
struct RgStream {
    rg_index: usize,
    stream: SendableRecordBatchStream,
    /// Global row index of the first row the next yielded batch will carry.
    next_row_offset: i64,
    exhausted: bool,
    /// Covering batch retained across an `RC_OVERFLOW` so the caller's grow-and-retry re-copies the
    /// same batch. `(first_row, last_row, batch)`.
    pending: Option<(i64, i64, RecordBatch)>,
}

/// One open DataFusion doc-values iterator over a single column.
struct DfIter {
    config: RowGroupStreamConfig,
    /// `rg_first_row[i]` = global row index of the first row of row group `i`; last entry is the
    /// total row count (sentinel), so `rg_first_row.len() == num_row_groups + 1`.
    rg_first_row: Vec<i64>,
    current: Option<RgStream>,
}

static ITERS: OnceLock<Mutex<HashMap<i64, DfIter>>> = OnceLock::new();
static NEXT_HANDLE: AtomicI64 = AtomicI64::new(0);

fn iters<'a>() -> Result<MutexGuard<'a, HashMap<i64, DfIter>>, String> {
    ITERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "df_docvalues: iterator registry mutex poisoned".to_string())
}

/// Open a single-column DataFusion doc-values iterator over `filename`, returning an opaque handle.
pub fn open(filename: &str, column: &str) -> Result<i64, String> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let location = object_store::path::Path::from_filesystem_path(filename)
        .map_err(|e| format!("df_docvalues: bad path {filename}: {e}"))?;
    let store_url =
        ObjectStoreUrl::parse("file://").map_err(|e| format!("df_docvalues: store url: {e}"))?;

    let (schema, file_size, metadata) =
        runtime().block_on(load_parquet_metadata(Arc::clone(&store), &location))?;

    let col_idx = schema
        .index_of(column)
        .map_err(|e| format!("df_docvalues: column {column} not in schema: {e}"))?;

    // Prefix sum of per-row-group row counts.
    let num_rgs = metadata.num_row_groups();
    let mut rg_first_row = Vec::with_capacity(num_rgs + 1);
    let mut acc: i64 = 0;
    for rg in metadata.row_groups() {
        rg_first_row.push(acc);
        acc += rg.num_rows();
    }
    rg_first_row.push(acc);

    let config = RowGroupStreamConfig {
        file_path: location.to_string(),
        file_size,
        store,
        store_url,
        full_schema: schema,
        metadata,
        projection: vec![col_idx],
        io_stats: Arc::new(ReadIoStats::default()),
    };

    let handle = NEXT_HANDLE.fetch_add(1, Ordering::SeqCst);
    iters()?.insert(
        handle,
        DfIter {
            config,
            rg_first_row,
            current: None,
        },
    );
    Ok(handle)
}

/// Close and drop an iterator handle. A no-op for an unknown handle.
pub fn close(handle: i64) {
    if let Ok(mut guard) = iters() {
        guard.remove(&handle);
    }
}

/// Number of currently open iterators (debug/leak checks).
pub fn open_count() -> i64 {
    iters().map(|g| g.len() as i64).unwrap_or(0)
}

impl DfIter {
    /// Row group containing global `row` (binary search over `rg_first_row`), or `None` if out of
    /// range.
    fn row_group_of(&self, row: i64) -> Option<usize> {
        let n = self.rg_first_row.len();
        if n < 2 || row < 0 || row >= self.rg_first_row[n - 1] {
            return None;
        }
        // partition_point: first index whose first_row > row, minus 1.
        let idx = self.rg_first_row.partition_point(|&first| first <= row);
        Some(idx - 1)
    }

    /// Build a fresh stream for the row group containing `target_row`, positioned at that row via a
    /// `skip(offset) select(rest)` `RowSelection`.
    fn open_rg_stream(&mut self, target_row: i64) -> Result<(), String> {
        let rg_index = self
            .row_group_of(target_row)
            .ok_or_else(|| format!("df_docvalues: row {target_row} out of range"))?;
        let rg_first = self.rg_first_row[rg_index];
        let rg_rows = (self.rg_first_row[rg_index + 1] - rg_first) as usize;
        let offset = (target_row - rg_first) as usize;

        let mut selectors = Vec::with_capacity(2);
        if offset > 0 {
            selectors.push(RowSelector::skip(offset));
        }
        selectors.push(RowSelector::select(rg_rows - offset));
        let selection = RowSelection::from(selectors);

        let stream = create_row_selection_stream(&self.config, rg_index, selection)
            .map_err(|e| format!("df_docvalues: create_row_selection_stream rg={rg_index}: {e}"))?;

        self.current = Some(RgStream {
            rg_index,
            stream,
            next_row_offset: target_row, // first delivered row is the selection's first selected row
            exhausted: false,
            pending: None,
        });
        Ok(())
    }
}

/// Advance to the batch containing `target_row` (seeking to its row group / page as needed) and
/// copy it into the caller's out-buffers. Buffer contract mirrors `parquet_decode_page_at_row`.
///
/// # Safety
/// Out pointers must be valid for writes of their stated capacities.
#[allow(clippy::too_many_arguments)]
pub unsafe fn next_batch(
    handle: i64,
    target_row: i64,
    out_first_row: *mut i64,
    out_last_row: *mut i64,
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> Result<i64, String> {
    let mut guard = iters()?;
    let it = guard
        .get_mut(&handle)
        .ok_or_else(|| format!("df_docvalues::next_batch: unknown handle {handle}"))?;

    // Determine the row group for target_row and (re)build the stream if needed: no current stream,
    // a different row group, or the current stream already advanced past target_row.
    let need_new = match &it.current {
        None => true,
        Some(cur) => {
            let target_rg = it.row_group_of(target_row);
            target_rg != Some(cur.rg_index)
                || cur.next_row_offset > target_row && cur.pending.is_none()
        }
    };
    if need_new {
        it.open_rg_stream(target_row)?;
    }

    let it = guard.get_mut(&handle).unwrap();
    let cur = it.current.as_mut().unwrap();

    // Re-copy a staged batch (prior RC_OVERFLOW for this same row).
    if let Some((first, last, _)) = cur.pending {
        if first <= target_row && target_row <= last {
            let (first, last, batch) = cur.pending.as_ref().unwrap();
            let rc = copy_batch_out(
                batch.column(0),
                *first,
                *last,
                out_first_row,
                out_last_row,
                out_value_buf,
                out_value_buf_cap,
                out_value_actual_len,
                out_presence_bitset,
                out_presence_bits_cap,
            )?;
            if rc == RC_OK {
                cur.pending = None;
            }
            return Ok(rc);
        }
        cur.pending = None;
    }

    // Pull batches forward within this row group until one covers target_row.
    loop {
        if cur.exhausted {
            return Ok(RC_EOF);
        }
        let batch: Option<RecordBatch> = runtime()
            .block_on(cur.stream.next())
            .transpose()
            .map_err(|e| {
                format!("df_docvalues::next_batch: stream error at row {target_row}: {e}")
            })?;
        let batch = match batch {
            Some(b) => b,
            None => {
                cur.exhausted = true;
                return Ok(RC_EOF);
            }
        };
        let rows = batch.num_rows() as i64;
        if rows == 0 {
            continue;
        }
        let first = cur.next_row_offset;
        let last = first + rows - 1;
        cur.next_row_offset = last + 1;

        if last < target_row {
            continue; // batch below the requested row — skip without copying
        }

        let rc = copy_batch_out(
            batch.column(0),
            first,
            last,
            out_first_row,
            out_last_row,
            out_value_buf,
            out_value_buf_cap,
            out_value_actual_len,
            out_presence_bitset,
            out_presence_bits_cap,
        )?;
        cur.pending = if rc == RC_OK {
            None
        } else {
            Some((first, last, batch))
        };
        return Ok(rc);
    }
}

/// Copy one Arrow column batch into the FFM out-buffers as `i64` values + LSB-first presence
/// bitset. Returns `RC_OVERFLOW` if a buffer is too small (after populating range + length).
#[allow(clippy::too_many_arguments)]
unsafe fn copy_batch_out(
    col: &ArrayRef,
    first: i64,
    last: i64,
    out_first_row: *mut i64,
    out_last_row: *mut i64,
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> Result<i64, String> {
    let len = col.len();
    let value_bytes = (len * 8) as i64;
    if !out_first_row.is_null() {
        *out_first_row = first;
    }
    if !out_last_row.is_null() {
        *out_last_row = last;
    }
    if !out_value_actual_len.is_null() {
        *out_value_actual_len = value_bytes;
    }
    let presence_words = ((len + 63) / 64) as i64;
    if value_bytes > out_value_buf_cap
        || out_value_buf.is_null()
        || presence_words > out_presence_bits_cap
        || out_presence_bitset.is_null()
    {
        return Ok(RC_OVERFLOW);
    }
    if len == 0 {
        return Ok(RC_OK);
    }

    let values = as_i64_words(col)?;
    debug_assert_eq!(values.len(), len);
    std::ptr::copy_nonoverlapping(values.as_ptr() as *const u8, out_value_buf, len * 8);

    let presence_bytes = (presence_words as usize) * 8;
    std::ptr::write_bytes(out_presence_bitset as *mut u8, 0, presence_bytes);
    let presence_dst = out_presence_bitset as *mut u8;
    match col.nulls() {
        None => {
            let full = len / 8;
            std::ptr::write_bytes(presence_dst, 0xFF, full);
            let rem = len % 8;
            if rem > 0 {
                *presence_dst.add(full) = ((1u16 << rem) - 1) as u8;
            }
        }
        Some(nb) => {
            let validity: &[u8] = nb.inner().values();
            let n = validity.len().min(presence_bytes);
            std::ptr::copy_nonoverlapping(validity.as_ptr(), presence_dst, n);
        }
    }
    Ok(RC_OK)
}

/// Convert a supported primitive Arrow array to raw `i64` words (INT sign-extended, float/double as
/// raw IEEE-754 bits, bool as 0/1). Null slots hold 0 (never read by Java, which gates on presence).
fn as_i64_words(col: &ArrayRef) -> Result<Vec<i64>, String> {
    let len = col.len();
    let mut out = vec![0i64; len];
    match col.data_type() {
        DataType::Int64 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::Int64Type>(),
            |v| v,
        ),
        DataType::Int32 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::Int32Type>(),
            |v| v as i64,
        ),
        DataType::Int16 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::Int16Type>(),
            |v| v as i64,
        ),
        DataType::Int8 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::Int8Type>(),
            |v| v as i64,
        ),
        DataType::UInt64 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::UInt64Type>(),
            |v| v as i64,
        ),
        DataType::UInt32 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::UInt32Type>(),
            |v| v as i64,
        ),
        DataType::UInt16 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::UInt16Type>(),
            |v| v as i64,
        ),
        DataType::UInt8 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::UInt8Type>(),
            |v| v as i64,
        ),
        DataType::Date32 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::Date32Type>(),
            |v| v as i64,
        ),
        DataType::Date64 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::Date64Type>(),
            |v| v,
        ),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::TimestampNanosecondType>(),
            |v| v,
        ),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::TimestampMicrosecondType>(),
            |v| v,
        ),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::TimestampMillisecondType>(),
            |v| v,
        ),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::TimestampSecondType>(),
            |v| v,
        ),
        DataType::Float64 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::Float64Type>(),
            |v| v.to_bits() as i64,
        ),
        DataType::Float32 => fill(
            &mut out,
            col.as_primitive::<arrow::datatypes::Float32Type>(),
            |v| (v.to_bits() as u64) as i64,
        ),
        DataType::Boolean => {
            let a = col.as_boolean();
            for i in 0..len {
                if a.is_valid(i) {
                    out[i] = if a.value(i) { 1 } else { 0 };
                }
            }
        }
        other => return Err(format!("df_docvalues: unsupported column type {other:?}")),
    }
    Ok(out)
}

/// Fill `out[i]` with `map(array.value(i))` for valid rows; null rows keep the pre-zeroed 0.
#[inline]
fn fill<T, F>(out: &mut [i64], array: &arrow::array::PrimitiveArray<T>, map: F)
where
    T: arrow::datatypes::ArrowPrimitiveType,
    F: Fn(T::Native) -> i64,
{
    for i in 0..array.len() {
        if array.is_valid(i) {
            out[i] = map(array.value(i));
        }
    }
}
