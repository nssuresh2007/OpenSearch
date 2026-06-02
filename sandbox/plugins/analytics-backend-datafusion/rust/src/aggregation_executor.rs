/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Aggregation executor — decodes a Substrait aggregation plan, builds an
//! `IndexedTableProvider` with a bitset-provider-backed `EvaluatorFactory`,
//! executes the plan, and serialises result `RecordBatch`es to Arrow IPC bytes.
//!
//! This is the Rust-side entry point for aggregation delegation. It mirrors
//! `execute_indexed_with_context` but is specialized for aggregation plans
//! shaped as `Aggregate(Filter(Scan))` where the Filter is the row filter
//! derived from a per-segment `Doc_ID_Bitset`.
//!
//! The bitset provider key is used to call back into Java via the existing
//! FFM callback table to get the packed u64 words for the row filter.

use std::sync::Arc;

use datafusion::{
    common::DataFusionError,
    physical_plan::execute_stream,
};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use futures::StreamExt;
use log::{error, info};
use prost::Message;
use substrait::proto::Plan;

use crate::indexed_table::eval::RowGroupBitsetSource;
use crate::indexed_table::eval::single_collector::SingleCollectorEvaluator;
use crate::indexed_table::ffm_callbacks::FfmSegmentCollector;
use crate::indexed_table::metrics::StreamMetrics;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::segment_info::build_segments;
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};
use crate::session_context::SessionContextHandle;

/// Execute an aggregation plan against a pre-configured SessionContext.
///
/// # Safety
///
/// `session_ctx_ptr` must be a valid pointer to a heap-allocated
/// `SessionContextHandle` produced by `df_create_session_context_indexed`
/// or equivalent. The pointer is consumed (Box::from_raw) — the caller
/// must not use it after this call returns.
///
/// # Arguments
///
/// * `session_ctx_ptr` — raw pointer to a `SessionContextHandle` (from FFM bridge)
/// * `substrait_bytes` — Substrait plan bytes encoding `Aggregate(Filter(Scan))`
/// * `provider_key` — FFM callback key for the bitset provider (registered on Java side)
/// * `writer_generation` — segment identifier for the bitset provider's collector
///
/// # Returns
///
/// Arrow IPC stream-format bytes containing the aggregation result `RecordBatch`es.
pub async unsafe fn execute_aggregation_with_context(
    session_ctx_ptr: i64,
    substrait_bytes: Vec<u8>,
    provider_key: i32,
    writer_generation: i64,
) -> Result<Vec<u8>, DataFusionError> {
    let handle = *Box::from_raw(session_ctx_ptr as *mut SessionContextHandle);

    // Per-query identifier; routed through every FFM upcall so Java can
    // dispatch callbacks to the matching `FilterDelegationHandle`.
    let context_id = handle.query_context.context_id();

    let ctx = handle.ctx;
    let table_name = handle.table_name;
    let table_path = handle.table_path;
    let object_metas = handle.object_metas;
    let writer_generations = handle.writer_generations;
    let query_config = Arc::new(handle.query_config);

    // Deregister the default ListingTable — will be replaced with IndexedTableProvider.
    ctx.deregister_table(&table_name)?;

    let store = ctx
        .state()
        .runtime_env()
        .object_store(&table_path)?;

    let state = ctx.state();
    let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();

    let (segments, schema) = build_segments(
        &state,
        Arc::clone(&store),
        object_metas.as_ref(),
        writer_generations.as_ref(),
        metadata_cache,
    )
    .await
    .map_err(|e| DataFusionError::Execution(format!("build_segments: {}", e)))?;
    let schema = crate::schema_coerce::coerce_inferred_schema(schema);

    // Write diagnostics to a file since eprintln/info may not be visible
    let diag_path = "/tmp/agg_executor_trace.log";
    let _ = std::fs::write(diag_path, format!(
        "full parquet schema ({} columns): {:?}\n",
        schema.fields().len(),
        schema.fields().iter().map(|f| format!("{}:{}", f.name(), f.data_type())).collect::<Vec<_>>()
    ));

    // Filter to only the target segment identified by writer_generation.
    // Java calls this function once per segment, so we only process the
    // segment whose bitset was registered under provider_key.
    let segments: Vec<_> = segments
        .into_iter()
        .filter(|seg| seg.writer_generation == writer_generation)
        .collect();
    if segments.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "No segment found with writer_generation={}",
            writer_generation
        )));
    }

    // Build the EvaluatorFactory that uses the bitset provider for row filtering.
    // The provider_key identifies the pre-registered bitset on the Java side.
    // For each segment chunk, we create a collector that reads the packed u64 words
    // from the Java-side FixedBitSet via FFM callbacks.
    //
    // Note: We do NOT wrap provider_key in a ProviderHandle here because the Java
    // side owns the provider lifecycle (BitsetProviderRegistry manages creation and
    // release). We only use the key to create per-chunk collectors.
    let factory: EvaluatorFactory = {
        let schema_for_pruner = schema.clone();
        let call_strategy = query_config.single_collector_strategy;

        Arc::new(
            move |segment: &SegmentFileInfo, chunk, stream_metrics: &StreamMetrics| {
                let collector = FfmSegmentCollector::create(
                    context_id,
                    provider_key,
                    segment.writer_generation,
                    chunk.doc_min,
                    chunk.doc_max,
                )
                .map_err(|e| {
                    format!(
                        "FfmSegmentCollector::create(context_id={}, provider={}, writer_generation={}, doc_range=[{},{})): {}",
                        context_id,
                        provider_key,
                        segment.writer_generation,
                        chunk.doc_min,
                        chunk.doc_max,
                        e
                    )
                })?;

                let pruner = Arc::new(PagePruner::new(
                    &schema_for_pruner,
                    Arc::clone(&segment.metadata),
                ));

                let eval: Arc<dyn RowGroupBitsetSource> =
                    Arc::new(SingleCollectorEvaluator::new(
                        Some(Arc::new(collector)),
                        pruner,
                        None, // no residual pruning predicate for aggregation
                        None, // no residual expression for aggregation
                        None, // no page prune metrics
                        stream_metrics.ffm_collector_calls.clone(),
                        call_strategy,
                        Arc::new(std::collections::HashMap::new()), // no performance-delegated providers
                        segment.writer_generation,
                        Arc::new(crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory),
                        context_id,
                    ));
                Ok(eval)
            },
        )
    };

    // Parse the store URL from the table path.
    let url_str = table_path.as_str();
    let parsed = url::Url::parse(url_str)
        .map_err(|e| DataFusionError::Execution(format!("parse table_path URL: {}", e)))?;
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::parse(
        format!("{}://{}", parsed.scheme(), parsed.authority()),
    )?;

    // ── Decode Substrait plan, register IndexedTableProvider, and execute ────
    //
    // The IndexedTableProvider is registered with the full Parquet schema so that
    // ParquetSource can correctly read columns from the file.
    //
    // The Substrait plan's base_schema declares only the columns referenced by the
    // aggregation (e.g., [revenue: BIGINT]). DataFusion's Substrait consumer
    // (`from_read_rel` → `apply_projection`) uses column NAMES from base_schema to
    // find their positional indices in the registered table's full schema. So even
    // though the plan references field 0 in the aggregate, the consumer creates a
    // TableScan with projection=[5] (the index of "revenue" in the full schema).
    // The aggregate then correctly operates on field 0 of the projected scan output.
    let plan = Plan::decode(substrait_bytes.as_slice()).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;

    // Log the base_schema from the plan for diagnostics.
    if let Some(base_schema) = crate::api::base_schema_for_table(&plan, &table_name) {
        let _ = std::fs::OpenOptions::new().append(true).open(diag_path)
            .and_then(|mut f| {
                use std::io::Write;
                writeln!(f, "plan base_schema for table '{}': names={:?}", table_name, base_schema.names)
            });
    } else {
        let _ = std::fs::OpenOptions::new().append(true).open(diag_path)
            .and_then(|mut f| {
                use std::io::Write;
                writeln!(f, "plan has NO base_schema for table '{}'", table_name)
            });
    }

    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store: Arc::clone(&store),
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None, // aggregation plans don't need pushdown predicates
        query_config: Arc::clone(&query_config),
        predicate_columns: vec![], // no predicate columns for aggregation
        emit_row_ids: false,
    }));
    ctx.register_table(&table_name, provider)?;

    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    let _ = std::fs::OpenOptions::new().append(true).open(diag_path)
        .and_then(|mut f| {
            use std::io::Write;
            writeln!(f, "logical plan:\n{}", logical_plan.display_indent())
        });

    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let _ = std::fs::OpenOptions::new().append(true).open(diag_path)
        .and_then(|mut f| {
            use std::io::Write;
            writeln!(f, "physical plan:\n{}", datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true))
        });

    // Retag bit-compatible Int↔UInt output mismatches.
    let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
    let physical_plan = crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;

    // Capture the output schema before execution consumes the plan.
    let output_schema = physical_plan.schema();

    // Execute and collect all result batches.
    let mut stream = execute_stream(physical_plan, ctx.task_ctx())
        .map_err(|e| {
            error!("execute_aggregation_with_context: failed to create stream: {}", e);
            e
        })?;

    let mut batches = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| {
            error!("execute_aggregation_with_context: batch error: {}", e);
            e
        })?;
        batches.push(batch);
    }

    // Write batch diagnostics to file
    let _ = std::fs::OpenOptions::new().append(true).open(diag_path)
        .and_then(|mut f| {
            use std::io::Write;
            writeln!(f, "execution complete: {} batches, total_rows={}",
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            )?;
            for (i, batch) in batches.iter().enumerate() {
                let schema = batch.schema();
                writeln!(f, "batch[{}]: rows={}, columns={:?}", i, batch.num_rows(),
                    schema.fields().iter().map(|field| format!("{}:{}", field.name(), field.data_type())).collect::<Vec<_>>()
                )?;
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let field = schema.field(col_idx);
                    writeln!(f, "  batch[{}].{}: is_null(0)={}, debug={:?}",
                        i, field.name(),
                        if col.len() > 0 { col.is_null(0) } else { true },
                        col
                    )?;
                }
            }
            Ok(())
        });

    info!(
        "[AGG_EXECUTOR_TRACE] execution complete: {} batches, total_rows={}",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    for (i, batch) in batches.iter().enumerate() {
        info!(
            "[AGG_EXECUTOR_TRACE] batch[{}]: schema={:?}, rows={}, null_counts={:?}",
            i,
            batch.schema().fields().iter().map(|f| format!("{}:{}", f.name(), f.data_type())).collect::<Vec<_>>(),
            batch.num_rows(),
            (0..batch.num_columns()).map(|c| {
                let col = batch.column(c);
                format!("{}:nulls={},len={}", batch.schema().field(c).name(), col.null_count(), col.len())
            }).collect::<Vec<_>>()
        );
        // Print actual values for small batches
        if batch.num_rows() <= 10 {
            let schema = batch.schema();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let field = schema.field(col_idx);
                info!(
                    "[AGG_EXECUTOR_TRACE] batch[{}].{}: is_null(0)={}, array_debug={:?}",
                    i, field.name(),
                    if col.len() > 0 { col.is_null(0) } else { true },
                    col
                );
            }
        }
    }

    // Serialize result RecordBatches to Arrow IPC stream format.
    if batches.is_empty() {
        // Return an empty IPC stream with the aggregation output schema.
        return serialize_batches_to_ipc(&[], &output_schema);
    }

    let result_schema = batches[0].schema();
    serialize_batches_to_ipc(&batches, &result_schema)
}

/// Serializes a slice of `RecordBatch`es into Arrow IPC stream-format bytes.
///
/// The output is a complete IPC stream: schema message, followed by one
/// record-batch message per batch, followed by the EOS marker. Java reads
/// this via `ArrowStreamReader` / `MessageChannelReader`.
fn serialize_batches_to_ipc(
    batches: &[arrow::array::RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<u8>, DataFusionError> {
    use arrow::ipc::writer::StreamWriter;

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| DataFusionError::Execution(format!("IPC StreamWriter::try_new: {}", e)))?;
        for batch in batches {
            writer.write(batch).map_err(|e| {
                DataFusionError::Execution(format!("IPC StreamWriter::write: {}", e))
            })?;
        }
        writer.finish().map_err(|e| {
            DataFusionError::Execution(format!("IPC StreamWriter::finish: {}", e))
        })?;
    }
    Ok(buf)
}
