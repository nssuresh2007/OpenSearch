/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Single-row-group `RowSelection` Parquet scan — a trimmed, local form of the indexed
//! executor's `indexed_table::parquet_bridge::create_row_selection_stream`.
//!
//! This is the decode primitive for the DataFusion doc-values path (Task 3.3): given a row group
//! and a `RowSelection`, it builds a DataFusion `ParquetSource` scan restricted to that row group
//! and executes it, so only the pages overlapping the selection are read. It is deliberately a
//! close copy of the proven indexed-executor code (same DataFusion version) so behavior matches.
//!
//! Trimmed relative to the original: no predicate pushdown, no `selectivity` gate, and **no
//! `LiquidParquetSource` wrapping / scoped page index** — those are folded back in when this is
//! lifted into the shared `df-parquet-access` crate (spec Task 3.1) and the shared liquid instance
//! is wired (Task 3.4). Until then the codec path decodes through a plain `ParquetSource`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::parquet::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory, RowGroupAccess,
};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::{ObjectStore, ObjectStoreExt};

/// Load footer-only Parquet metadata for `location` in `store`. Page index bytes are read
/// opportunistically (`Optional`) — never a separate IO request. No metadata cache for now (the
/// iterator holds the returned `Arc<ParquetMetaData>` for its lifetime); a shared cache is added
/// with the `df-parquet-access` extraction (spec Task 3.1).
pub async fn load_parquet_metadata(
    store: Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
) -> std::result::Result<(SchemaRef, u64, Arc<ParquetMetaData>), String> {
    let meta = store
        .head(location)
        .await
        .map_err(|e| format!("object-store head {location}: {e}"))?;
    let size = meta.size as u64;

    let mut reader = ParquetObjectReader::new(Arc::clone(&store), location.clone());
    let pq_meta = Arc::new(
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .load_and_finish(&mut reader, size)
            .await
            .map_err(|e| format!("load parquet metadata {location}: {e}"))?,
    );

    let file_meta = pq_meta.file_metadata();
    let schema = parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
        .map_err(|e| format!("parquet_to_arrow_schema {location}: {e}"))?;
    Ok((Arc::new(schema), size, pq_meta))
}

/// Shared accumulator for object-store read wall-time.
#[derive(Debug, Default)]
pub struct ReadIoStats {
    pub total_ns: AtomicU64,
    pub count: AtomicU64,
}

fn record_io(stats: &ReadIoStats, dur: Duration) {
    stats
        .total_ns
        .fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
    stats.count.fetch_add(1, Ordering::Relaxed);
}

/// Configuration for a per-row-group Parquet stream (single-column doc-values projection).
pub struct RowGroupStreamConfig {
    pub file_path: String,
    pub file_size: u64,
    pub store: Arc<dyn ObjectStore>,
    pub store_url: ObjectStoreUrl,
    pub full_schema: SchemaRef,
    pub metadata: Arc<ParquetMetaData>,
    pub projection: Vec<usize>,
    pub io_stats: Arc<ReadIoStats>,
}

/// Build a stream that reads a single row group using `selection`. Only the pages overlapping the
/// selection are fetched/decoded.
pub fn create_row_selection_stream(
    config: &RowGroupStreamConfig,
    rg_index: usize,
    selection: RowSelection,
) -> Result<SendableRecordBatchStream> {
    let num_rgs = config.metadata.num_row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(num_rgs);
    access_plan.set(rg_index, RowGroupAccess::Selection(selection));

    let partitioned_file = PartitionedFile::new(config.file_path.clone(), config.file_size)
        .with_extension(Arc::new(access_plan));

    let reader_factory = Arc::new(CachedMetadataReaderFactory::new(
        Arc::clone(&config.store),
        Arc::clone(&config.metadata),
        Arc::clone(&config.io_stats),
    )) as Arc<dyn ParquetFileReaderFactory>;

    let source = ParquetSource::new(config.full_schema.clone())
        .with_parquet_file_reader_factory(reader_factory)
        .with_enable_page_index(false);

    let config_builder = FileScanConfigBuilder::new(config.store_url.clone(), Arc::new(source))
        .with_file(partitioned_file)
        .with_projection_indices(Some(config.projection.clone()))?;

    let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config_builder.build());
    let ctx = Arc::new(datafusion::execution::TaskContext::default());
    exec.execute(0, ctx)
}

/// Factory that creates Parquet readers with pre-cached metadata (avoids re-reading the footer).
#[derive(Debug)]
pub struct CachedMetadataReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata: Arc<ParquetMetaData>,
    io_stats: Arc<ReadIoStats>,
}

impl CachedMetadataReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata: Arc<ParquetMetaData>,
        io_stats: Arc<ReadIoStats>,
    ) -> Self {
        Self {
            store,
            metadata,
            io_stats,
        }
    }
}

impl ParquetFileReaderFactory for CachedMetadataReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file.object_meta.location.as_ref(), metrics);
        Ok(Box::new(CachedMetadataReader {
            store: Arc::clone(&self.store),
            location: file.object_meta.location.clone(),
            metadata: Arc::clone(&self.metadata),
            metrics: file_metrics,
            io_stats: Arc::clone(&self.io_stats),
        }))
    }
}

struct CachedMetadataReader {
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    metadata: Arc<ParquetMetaData>,
    metrics: ParquetFileMetrics,
    io_stats: Arc<ReadIoStats>,
}

impl AsyncFileReader for CachedMetadataReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        self.metrics
            .bytes_scanned
            .add((range.end - range.start) as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let io_stats = Arc::clone(&self.io_stats);
        async move {
            let t0 = Instant::now();
            let r = store
                .get_range(&location, range)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)));
            record_io(&io_stats, t0.elapsed());
            r
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.metrics.bytes_scanned.add(total as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let io_stats = Arc::clone(&self.io_stats);
        async move {
            let t0 = Instant::now();
            let r = store
                .get_ranges(&location, &ranges)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)));
            record_io(&io_stats, t0.elapsed());
            r
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(&self.metadata);
        async move { Ok(metadata) }.boxed()
    }
}
