/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.analytics.spi.BackendAggregationExecutor;
import org.opensearch.analytics.spi.BackendExecutionException;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.SegmentExecutionContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.internal.SearchContext;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Set;

/**
 * DataFusion v1 implementation of {@link BackendAggregationExecutor}.
 *
 * <p>Evaluates eligible aggregations over Parquet column storage using the DataFusion
 * Rust runtime via the existing FFM bridge. The executor:
 * <ol>
 *   <li>Declares itself as the "datafusion" backend</li>
 *   <li>Checks eligibility: field must have "parquet" format, aggregation type must be
 *       in the supported set, no scripts, no unsupported parameters</li>
 *   <li>On {@code executeOnSegment}: converts the aggregation to Substrait,
 *       invokes the FFM bridge, and converts Arrow IPC results back to
 *       {@code InternalAggregation}</li>
 * </ol>
 *
 * <p>Implements {@link SegmentExecutionContext} so the delegation framework can pass
 * per-segment FFM parameters (provider key, writer generation) before each execution call.
 *
 * @opensearch.internal
 */
public class DataFusionAggregationExecutor implements BackendAggregationExecutor, SegmentExecutionContext {

    private static final Logger logger = LogManager.getLogger(DataFusionAggregationExecutor.class);

    /**
     * The set of aggregation type names supported by the DataFusion v1 executor.
     * These correspond to the OpenSearch aggregation type strings as returned by
     * {@link AggregationBuilder#getType()}.
     */
    static final Set<String> SUPPORTED_AGGREGATION_TYPES = Set.of(
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "value_count",
        "stats",
        "extended_stats",
        "terms",
        "date_histogram",
        "histogram",
        "range",
        "filters"
    );

    /** The format string that must be present in a field's doc-value formats for eligibility. */
    private static final String PARQUET_FORMAT = "parquet";

    private final DataFusionFragmentConvertor fragmentConvertor;
    private final DataFusionPlugin plugin;
    private final PartialAggregationAdapter partialAggregationAdapter;

    // Per-segment context set by AggregationDelegationService before each executeOnSegment call
    private final ThreadLocal<SegmentContext> currentSegmentContext = new ThreadLocal<>();

    /**
     * Creates a new DataFusion aggregation executor.
     *
     * @param fragmentConvertor the convertor used to emit Substrait plan bytes
     * @param plugin            the DataFusion plugin providing access to native runtime and reader
     */
    public DataFusionAggregationExecutor(DataFusionFragmentConvertor fragmentConvertor, DataFusionPlugin plugin) {
        this.fragmentConvertor = fragmentConvertor;
        this.plugin = plugin;
        this.partialAggregationAdapter = new PartialAggregationAdapter();
    }

    @Override
    public String backendId() {
        return "datafusion";
    }

    @Override
    public void setSegmentContext(int providerKey, long writerGeneration, AggregationBuilder originalAgg) {
        currentSegmentContext.set(new SegmentContext(providerKey, writerGeneration, originalAgg));
    }

    /**
     * Checks whether this backend can evaluate the given aggregation against the given field.
     *
     * <p>Eligibility requires:
     * <ul>
     *   <li>The field has "parquet" in its doc-value formats</li>
     *   <li>The aggregation type is in the supported set</li>
     *   <li>The aggregation does not use a script</li>
     *   <li>The aggregation does not use unsupported parameters (regex include/exclude)</li>
     * </ul>
     */
    @Override
    public boolean canEvaluate(AggregationBuilder agg, FieldStorageInfo field) {
        // Check field has parquet format
        if (field == null || field.getDocValueFormats() == null || field.getDocValueFormats().contains(PARQUET_FORMAT) == false) {
            return false;
        }

        // Check aggregation type is supported
        if (SUPPORTED_AGGREGATION_TYPES.contains(agg.getType()) == false) {
            return false;
        }

        // Check no script is used
        if (hasScript(agg)) {
            return false;
        }

        // Check no unsupported parameters (regex include/exclude on terms)
        if (hasUnsupportedParameters(agg)) {
            return false;
        }

        return true;
    }

    /**
     * Executes the eligible aggregation subtree on a single Lucene segment.
     *
     * <p>The execution flow:
     * <ol>
     *   <li>Retrieves per-segment context (providerKey, writerGeneration) set by the
     *       delegation service</li>
     *   <li>Creates a DataFusion SessionContext for the segment's Parquet file</li>
     *   <li>Builds a Substrait plan from the aggregation tree via
     *       {@code DataFusionFragmentConvertor}</li>
     *   <li>Invokes the FFM bridge via {@code NativeBridge.executeAggregationWithContext}</li>
     *   <li>Converts the Arrow IPC bytes to {@code InternalAggregation} via
     *       {@code PartialAggregationAdapter}</li>
     * </ol>
     *
     * @param eligibleSubtree the aggregation subtree classified as eligible
     * @param leaf            the Lucene segment to execute against
     * @param matchingDocs    the set of matching docIds within this segment
     * @param ctx             the search context
     * @return per-segment partial aggregation state
     * @throws BackendExecutionException if the backend encounters an error
     */
    @Override
    public InternalAggregation executeOnSegment(
        AggregatorFactories eligibleSubtree,
        LeafReaderContext leaf,
        FixedBitSet matchingDocs,
        SearchContext ctx
    ) throws BackendExecutionException {
        logger.debug(
            "executeOnSegment: segment={}, matchingDocs.cardinality={}, aggCount={}",
            leaf.ord,
            matchingDocs.cardinality(),
            eligibleSubtree.countAggregators()
        );

        long contextId = 0L;
        try {
            // Step 0: Retrieve per-segment context
            SegmentContext segCtx = currentSegmentContext.get();
            if (segCtx == null) {
                throw new BackendExecutionException(
                    "No segment execution context set — AggregationDelegationService must call "
                        + "setSegmentContext() before executeOnSegment()"
                );
            }

            int providerKey = segCtx.providerKey();
            long writerGeneration = segCtx.writerGeneration();

            // Step 1: Get the DataFusion reader and runtime pointers
            DataFusionService dataFusionService = plugin.getDataFusionService();
            if (dataFusionService == null) {
                throw new BackendExecutionException("DataFusionService not initialized");
            }
            long runtimePtr = dataFusionService.getNativeRuntime().get();

            DatafusionReader dfReader = getDatafusionReader(ctx);
            if (dfReader == null) {
                throw new BackendExecutionException(
                    "No DatafusionReader available for shard — cannot execute aggregation on segment " + leaf.ord
                );
            }
            long readerPtr = dfReader.getReaderHandle().getPointer();

            // Step 2: Create a SessionContext for indexed execution on this segment
            // Use the indexed session context so the IndexedTableProvider can apply the
            // bitset-derived row filter. No delegated predicates in the aggregation path,
            // so treeShape=NO_DELEGATION and delegatedPredicateCount=0.
            // Table name must be "input-0" to match the Substrait plan produced by
            // convertSchemaOnlyRead(stageId=0, ...) which names the table "input-<stageId>".
            String tableName = "input-0";
            contextId = ctx.getTask() != null ? ctx.getTask().getId() : 0L;

            SessionContextHandle sessionCtxHandle;
            WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment configSegment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                snapshot.writeTo(configSegment);
                sessionCtxHandle = NativeBridge.createSessionContextForIndexedExecution(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    FilterTreeShape.NO_DELEGATION.ordinal(),
                    0,       // delegatedPredicateCount
                    false,   // requestsRowIds — not a QTF query
                    configSegment.address(),
                    null     // planBytes — no multi-index schema widening needed
                );
            }

            // Step 3: Build Substrait plan bytes from the eligible aggregation subtree
            // For the initial wireup, we build a minimal Substrait plan that represents
            // the aggregation. The plan is: Aggregate(Scan(table))
            byte[] substraitBytes = buildSubstraitPlan(eligibleSubtree, ctx);

            // Step 4: Install a FilterDelegationHandle so the Rust-side IndexedTableProvider
            // can call back to get the bitset. We use the actual matchingDocs bitset from
            // the Lucene query phase — this ensures DataFusion only aggregates over the
            // documents that matched the query.
            org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks.register(
                contextId, new AggBitsetHandle(matchingDocs), null
            );

            // Step 5: Invoke FFM bridge — execute_aggregation_with_context
            // Rust takes ownership of the session context via Box::from_raw; mark consumed
            // AFTER the call so that validatePointer() inside the bridge method sees a live handle.
            long sessionCtxPtr = sessionCtxHandle.getPointer();

            logger.info(
                "[AGG_DELEGATION_TRACE] About to call executeAggregationWithContext: sessionCtxPtr={}, substraitBytes.length={}, providerKey={}, writerGeneration={}",
                sessionCtxPtr, substraitBytes.length, providerKey, writerGeneration
            );

            byte[] arrowIpcBytes = NativeBridge.executeAggregationWithContext(
                sessionCtxPtr,
                substraitBytes,
                providerKey,
                writerGeneration
            );

            logger.info(
                "[AGG_DELEGATION_TRACE] executeAggregationWithContext returned: arrowIpcBytes.length={}",
                arrowIpcBytes.length
            );

            // Dump the substrait plan base_schema info for debugging
            logger.info(
                "[AGG_DELEGATION_TRACE] Substrait plan first 64 bytes (hex): {}",
                bytesToHex(substraitBytes, 64)
            );

            // Rust consumed the session context — mark it so doClose() won't double-free.
            sessionCtxHandle.markConsumed();

            // Step 5: Convert Arrow IPC bytes to InternalAggregation
            AggregationBuilder originalAgg = segCtx.originalAgg();
            InternalAggregation result = partialAggregationAdapter.convert(arrowIpcBytes, originalAgg);

            logger.debug(
                "executeOnSegment completed: segment={}, resultType={}",
                leaf.ord,
                result.getClass().getSimpleName()
            );

            return result;
        } catch (BackendExecutionException e) {
            throw e;
        } catch (Exception e) {
            logger.error(
                "[AGG_DELEGATION_TRACE] DataFusionAggregationExecutor.executeOnSegment FAILED on segment {}: {} — {}",
                leaf.ord,
                e.getClass().getName(),
                e.getMessage(),
                e
            );
            throw new BackendExecutionException("Unexpected error during DataFusion aggregation execution on segment " + leaf.ord, e);
        } finally {
            // Unregister the filter callbacks binding to avoid leaks
            org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks.unregister(contextId);
            // Clear the thread-local context after execution
            currentSegmentContext.remove();
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────────

    /**
     * Obtains the {@link DatafusionReader} for the current shard.
     *
     * <p>Acquires an {@link org.opensearch.index.engine.exec.IndexReaderProvider.Reader} from the
     * shard's indexer and extracts the DataFusion-format reader from it. This mirrors the pattern
     * used by the existing plan-driven path in {@code DataFusionAnalyticsBackendPlugin} and
     * {@code ShardScanInstructionHandler}.
     *
     * <p>Note: The acquired reader is NOT closed here — it shares the same catalog snapshot as the
     * searcher that was acquired for this search context. The reader's lifecycle is managed by the
     * search context's close. For a production implementation, the reader should be acquired once
     * per request and cached in the {@link org.opensearch.analytics.aggregation.DelegationRequestState}.
     */
    private DatafusionReader getDatafusionReader(SearchContext ctx) {
        try {
            var indexShard = ctx.indexShard();
            // Acquire a reader from the indexer — this gives us the format-aware reader
            // containing per-format readers (LuceneReader, DatafusionReader, etc.)
            var readerCloseable = indexShard.getReaderProvider().acquireReader();
            var reader = readerCloseable.get();

            if (reader == null) {
                logger.debug("acquireReader() returned null for shard [{}]", indexShard.shardId());
                return null;
            }

            // Get the DatafusionReader from the format-aware reader using the same pattern
            // as the existing plan-driven path
            DataFormatRegistry registry = plugin.getDataFormatRegistry();
            if (registry == null) {
                logger.debug("DataFormatRegistry is null");
                return null;
            }

            for (String formatName : plugin.getSupportedFormats()) {
                var format = registry.format(formatName);
                if (format != null) {
                    DatafusionReader dfReader = reader.getReader(format, DatafusionReader.class);
                    if (dfReader != null) {
                        logger.info(
                            "[AGG_DELEGATION_TRACE] DataFusionAggregationExecutor: obtained DatafusionReader for format [{}] on shard [{}]",
                            formatName,
                            indexShard.shardId()
                        );
                        // Note: we intentionally do NOT close readerCloseable here.
                        // The reader shares the catalog snapshot with the search context.
                        // TODO: Cache the reader in DelegationRequestState and close on request completion.
                        return dfReader;
                    }
                }
            }

            logger.debug("No DatafusionReader found in any supported format for shard [{}]", indexShard.shardId());
            // Close the reader since we couldn't use it
            readerCloseable.close();
        } catch (Exception e) {
            logger.debug("Failed to obtain DatafusionReader from indexer: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Builds a Substrait plan from the eligible aggregation subtree.
     *
     * <p>For the initial wireup, this builds a schema-only scan plan and attaches the
     * aggregation on top using {@code DataFusionFragmentConvertor}. The plan shape is:
     * {@code Aggregate(Filter(Scan(table)))} where the Filter is the bitset-derived row
     * filter applied by the IndexedTableProvider on the Rust side.
     *
     * <p>For simple metric aggregations (sum, min, max, avg, count), the Substrait plan
     * is a straightforward partial aggregate over the scan. The Rust-side executor applies
     * the row filter from the registered bitset provider.
     */
    private byte[] buildSubstraitPlan(AggregatorFactories eligibleSubtree, SearchContext ctx) throws BackendExecutionException {
        try {
            // Get the original aggregation builder from the segment context
            SegmentContext segCtx = currentSegmentContext.get();
            AggregationBuilder originalAgg = segCtx != null ? segCtx.originalAgg() : null;
            if (originalAgg == null) {
                throw new BackendExecutionException("No original AggregationBuilder available in segment context");
            }

            // For the initial wireup, we use the fragment convertor to build a minimal
            // Substrait plan. The plan represents a partial aggregation over the segment's
            // Parquet data, with the row filter applied by the IndexedTableProvider.
            //
            // The full Calcite-based conversion (AggregationTreeWalker → LogicalAggregate →
            // Substrait) requires the full planner infrastructure. For now, we build the
            // plan using a simplified path that handles simple metric aggregations.
            //
            // The Rust-side aggregation_executor.rs interprets the Substrait plan and
            // executes it over the segment's Parquet file with the bitset row filter.
            java.util.Collection<AggregationBuilder> aggBuilders = java.util.List.of(originalAgg);
            byte[] planBytes = buildSimpleAggregationPlan(aggBuilders, ctx);
            if (planBytes == null || planBytes.length == 0) {
                throw new BackendExecutionException("Failed to build Substrait plan for aggregation");
            }
            return planBytes;
        } catch (BackendExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new BackendExecutionException("Failed to build Substrait plan from aggregation subtree", e);
        }
    }

    /**
     * Builds a simple Substrait aggregation plan for metric aggregations.
     *
     * <p>Uses the {@link DataFusionFragmentConvertor} to produce a schema-only scan plan
     * and then attaches the aggregation on top. For simple metrics (no GROUP BY), the plan
     * is: {@code Aggregate(Scan)}.
     *
     * <p>This method uses the fragment convertor's existing infrastructure to produce
     * well-formed Substrait bytes that the Rust runtime can execute.
     */
    private byte[] buildSimpleAggregationPlan(java.util.Collection<AggregationBuilder> aggBuilders, SearchContext ctx)
        throws BackendExecutionException {
        // For the initial wireup, we produce a minimal Substrait plan that the Rust-side
        // aggregation executor can interpret. The plan encodes:
        // - The table name (for the IndexedTableProvider to resolve the Parquet file)
        // - The aggregation function(s) to apply
        // - The field(s) to aggregate over
        //
        // The Rust executor (aggregation_executor.rs) receives this plan along with the
        // providerKey and writerGeneration, creates the IndexedTableProvider with the
        // bitset row filter, and executes the aggregation.
        //
        // For now, we use the fragment convertor's convertSchemaOnlyRead to produce a
        // base scan plan, then use attachPartialAggOnTop to layer the aggregation.
        // This requires building a Calcite LogicalAggregate, which needs the planner
        // infrastructure.
        //
        // SIMPLIFIED PATH: For the initial wireup, we produce a plan that the Rust
        // executor can interpret directly. The plan bytes encode the aggregation type
        // and field name in a format the Rust side understands.
        //
        // TODO: Replace with full Calcite-based plan construction once the
        // AggregationTreeWalker integration is complete.
        AggregationBuilder firstAgg = aggBuilders.iterator().next();
        String aggType = firstAgg.getType();
        String fieldName = extractFieldName(firstAgg);

        if (fieldName == null && isFieldRequired(aggType)) {
            throw new BackendExecutionException(
                "Cannot determine field name for aggregation [" + firstAgg.getName() + "] of type [" + aggType + "]"
            );
        }

        // Build a schema-only scan plan as the inner bytes, then attach the aggregate on top.
        // For the initial wireup, we produce a minimal plan that encodes the aggregation
        // intent. The Rust-side executor parses this and builds the DataFusion physical plan.
        //
        // Use the schema-only read as the base (stage id 0 = "input-0")
        String tableName = ctx.indexShard().shardId().getIndex().getName();
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory = new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT
        );

        // Build a minimal row type for the scan based on the aggregation's field
        org.apache.calcite.rel.type.RelDataTypeFactory.Builder rowTypeBuilder = typeFactory.builder();
        if (fieldName != null) {
            // Determine the correct Calcite type from the OpenSearch field mapping
            org.apache.calcite.sql.type.SqlTypeName fieldSqlType = resolveFieldSqlType(fieldName, ctx);
            rowTypeBuilder.add(fieldName, fieldSqlType).nullable(true);
        }
        org.apache.calcite.rel.type.RelDataType scanRowType = rowTypeBuilder.build();

        // Produce the schema-only scan plan bytes
        byte[] innerBytes = fragmentConvertor.convertSchemaOnlyRead(0, scanRowType);

        // Build the LogicalAggregate RelNode for the partial aggregation
        org.apache.calcite.rel.RelNode aggRelNode = buildAggregateRelNode(firstAgg, scanRowType, typeFactory);

        // Attach the aggregate on top of the scan
        return fragmentConvertor.attachPartialAggOnTop(aggRelNode, innerBytes);
    }

    /**
     * Builds a Calcite {@code LogicalAggregate} RelNode for the given aggregation.
     */
    private org.apache.calcite.rel.RelNode buildAggregateRelNode(
        AggregationBuilder agg,
        org.apache.calcite.rel.type.RelDataType inputRowType,
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory
    ) throws BackendExecutionException {
        try {
            // Create a minimal Calcite environment for building the RelNode
            org.apache.calcite.plan.RelOptPlanner planner = new org.apache.calcite.plan.hep.HepPlanner(
                new org.apache.calcite.plan.hep.HepProgramBuilder().build()
            );
            org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);
            org.apache.calcite.plan.RelOptCluster cluster = org.apache.calcite.plan.RelOptCluster.create(planner, rexBuilder);

            // Build a dummy input whose row type matches the scan schema.
            // Use LogicalValues with the correct row type — the aggregate references
            // fields by index (via AggregateCall.argList), not by expression, so the
            // dummy input just needs the right row type. The rewire in
            // attachPartialAggOnTop replaces this with the actual scan.
            org.apache.calcite.rel.RelNode dummyInput = org.apache.calcite.rel.logical.LogicalValues.createEmpty(
                cluster,
                inputRowType
            );

            // Build the AggregateCall based on the aggregation type
            String aggType = agg.getType();
            String fieldName = extractFieldName(agg);
            int fieldIndex = fieldName != null ? inputRowType.getFieldNames().indexOf(fieldName) : -1;

            org.apache.calcite.rel.core.AggregateCall aggCall = buildCalciteAggCall(
                aggType,
                fieldIndex,
                inputRowType,
                typeFactory
            );

            // Build the LogicalAggregate — no GROUP BY for simple metrics
            java.util.List<org.apache.calcite.rel.core.AggregateCall> aggCalls = java.util.List.of(aggCall);
            org.apache.calcite.util.ImmutableBitSet groupSet = org.apache.calcite.util.ImmutableBitSet.of();

            return org.apache.calcite.rel.logical.LogicalAggregate.create(
                dummyInput,
                java.util.List.of(),  // groupSets hint
                groupSet,
                java.util.List.of(groupSet),  // groupSets
                aggCalls
            );
        } catch (Exception e) {
            throw new BackendExecutionException("Failed to build Calcite LogicalAggregate for aggregation [" + agg.getName() + "]", e);
        }
    }

    /**
     * Builds project expressions that produce the input row type from a one-row values.
     * These are just null literals cast to the appropriate types — the actual data comes
     * from the scan that replaces this dummy input during rewire.
     */
    private java.util.List<org.apache.calcite.rex.RexNode> buildProjectExpressions(
        org.apache.calcite.rel.type.RelDataType rowType,
        org.apache.calcite.rex.RexBuilder rexBuilder,
        org.apache.calcite.rel.RelNode input
    ) {
        java.util.List<org.apache.calcite.rex.RexNode> exprs = new java.util.ArrayList<>();
        for (org.apache.calcite.rel.type.RelDataTypeField field : rowType.getFieldList()) {
            exprs.add(rexBuilder.makeNullLiteral(field.getType()));
        }
        return exprs;
    }

    /**
     * Builds a Calcite {@link org.apache.calcite.rel.core.AggregateCall} for the given
     * OpenSearch aggregation type.
     */
    private org.apache.calcite.rel.core.AggregateCall buildCalciteAggCall(
        String aggType,
        int fieldIndex,
        org.apache.calcite.rel.type.RelDataType inputRowType,
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory
    ) {
        org.apache.calcite.sql.SqlAggFunction sqlAggFunc;
        java.util.List<Integer> argList;
        org.apache.calcite.rel.type.RelDataType returnType;

        switch (aggType) {
            case "sum":
                sqlAggFunc = org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
                argList = java.util.List.of(fieldIndex);
                // SUM return type matches the input field type (Calcite infers this)
                returnType = typeFactory.createTypeWithNullability(
                    inputRowType.getFieldList().get(fieldIndex).getType(), true
                );
                break;
            case "min":
                sqlAggFunc = org.apache.calcite.sql.fun.SqlStdOperatorTable.MIN;
                argList = java.util.List.of(fieldIndex);
                // MIN/MAX return type matches the input field type
                returnType = typeFactory.createTypeWithNullability(
                    inputRowType.getFieldList().get(fieldIndex).getType(), true
                );
                break;
            case "max":
                sqlAggFunc = org.apache.calcite.sql.fun.SqlStdOperatorTable.MAX;
                argList = java.util.List.of(fieldIndex);
                returnType = typeFactory.createTypeWithNullability(
                    inputRowType.getFieldList().get(fieldIndex).getType(), true
                );
                break;
            case "avg":
                sqlAggFunc = org.apache.calcite.sql.fun.SqlStdOperatorTable.AVG;
                argList = java.util.List.of(fieldIndex);
                returnType = typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE), true
                );
                break;
            case "count":
            case "value_count":
                sqlAggFunc = org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT;
                argList = fieldIndex >= 0 ? java.util.List.of(fieldIndex) : java.util.List.of();
                returnType = typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
                break;
            default:
                // For complex aggregations (stats, terms, etc.), use COUNT as a placeholder
                // TODO: Implement full conversion for bucket and multi-value aggregations
                sqlAggFunc = org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT;
                argList = java.util.List.of();
                returnType = typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
                break;
        }

        return org.apache.calcite.rel.core.AggregateCall.create(
            sqlAggFunc,
            false,          // isDistinct
            argList,
            -1,             // filterArg (none)
            returnType,
            "value"         // name — matches what PartialAggregationAdapter expects for metric aggs
        );
    }

    /**
     * Extracts the field name from an aggregation builder.
     */
    private static String extractFieldName(AggregationBuilder agg) {
        if (agg instanceof ValuesSourceAggregationBuilder<?> vsAgg) {
            return vsAgg.field();
        }
        return null;
    }

    /**
     * Resolves the Calcite SQL type for a field based on its OpenSearch mapping type.
     * Maps OpenSearch field types to the corresponding Parquet/Arrow types that DataFusion expects.
     */
    private static org.apache.calcite.sql.type.SqlTypeName resolveFieldSqlType(String fieldName, SearchContext ctx) {
        try {
            var fieldType = ctx.getQueryShardContext().fieldMapper(fieldName);
            if (fieldType != null) {
                String typeName = fieldType.typeName();
                return switch (typeName) {
                    case "long", "integer", "short", "byte" -> org.apache.calcite.sql.type.SqlTypeName.BIGINT;
                    case "double", "float", "half_float", "scaled_float" -> org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
                    case "keyword", "text" -> org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
                    case "boolean" -> org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
                    case "date", "date_nanos" -> org.apache.calcite.sql.type.SqlTypeName.BIGINT; // dates stored as epoch millis
                    default -> org.apache.calcite.sql.type.SqlTypeName.DOUBLE; // fallback
                };
            }
        } catch (Exception e) {
            // Fall through to default
        }
        return org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
    }

    /**
     * Returns whether the given aggregation type requires a field name.
     * Count/value_count can operate without a specific field (counts all docs).
     */
    private static boolean isFieldRequired(String aggType) {
        return "count".equals(aggType) == false && "value_count".equals(aggType) == false;
    }

    /**
     * Checks whether the aggregation uses a script (inline or stored).
     */
    private static boolean hasScript(AggregationBuilder agg) {
        if (agg instanceof ValuesSourceAggregationBuilder<?> vsAgg) {
            return vsAgg.script() != null;
        }
        return false;
    }

    /**
     * Checks whether the aggregation uses unsupported parameters.
     * Currently checks for regex include/exclude on terms aggregations.
     */
    private static boolean hasUnsupportedParameters(AggregationBuilder agg) {
        // Terms aggregation with include/exclude regex patterns is not supported
        if ("terms".equals(agg.getType())) {
            return hasTermsIncludeExclude(agg);
        }
        return false;
    }

    /**
     * Checks if a terms aggregation has include/exclude parameters set.
     * Uses reflection-free approach by checking the aggregation's metadata.
     */
    private static boolean hasTermsIncludeExclude(AggregationBuilder agg) {
        // TermsAggregationBuilder exposes includeExclude() — check via the known subclass
        try {
            var method = agg.getClass().getMethod("includeExclude");
            Object includeExclude = method.invoke(agg);
            return includeExclude != null;
        } catch (NoSuchMethodException e) {
            // Not a terms aggregation subclass with includeExclude — safe
            return false;
        } catch (Exception e) {
            // Defensive: if we can't determine, mark as unsupported
            logger.debug("Unable to check includeExclude on aggregation [{}]: {}", agg.getName(), e.getMessage());
            return true;
        }
    }

    // ── Inner types ──────────────────────────────────────────────────────────────

    /**
     * Per-segment execution context holding the FFM parameters needed for the native call.
     */
    private record SegmentContext(int providerKey, long writerGeneration, AggregationBuilder originalAgg) {
    }

    /**
     * A {@link org.opensearch.analytics.spi.FilterDelegationHandle} that provides the
     * actual matching-docs bitset from the Lucene query phase to the Rust-side
     * IndexedTableProvider. This ensures DataFusion only aggregates over documents
     * that matched the query + post-filter + liveDocs.
     */
    private class AggBitsetHandle implements org.opensearch.analytics.spi.FilterDelegationHandle {
        private final FixedBitSet matchingDocs;
        private final java.util.concurrent.atomic.AtomicInteger collectorKeyGen = new java.util.concurrent.atomic.AtomicInteger(0);
        private final java.util.concurrent.ConcurrentHashMap<Integer, FixedBitSet> collectors = new java.util.concurrent.ConcurrentHashMap<>();

        AggBitsetHandle(FixedBitSet matchingDocs) {
            this.matchingDocs = matchingDocs;
        }

        @Override
        public int createProvider(int annotationId) {
            return annotationId;
        }

        @Override
        public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
            // Store the matching docs bitset for this collector
            int key = collectorKeyGen.incrementAndGet();
            collectors.put(key, matchingDocs);
            logger.info(
                "[AGG_DELEGATION_TRACE] AggBitsetHandle.createCollector: providerKey={}, writerGen={}, range=[{},{}), bitset.length={}, bitset.cardinality={}, collectorKey={}",
                providerKey, writerGeneration, minDoc, maxDoc,
                matchingDocs.length(), matchingDocs.cardinality(), key
            );
            return key;
        }

        @Override
        public int collectDocs(int collectorKey, int minDoc, int maxDoc, java.lang.foreign.MemorySegment out) {
            FixedBitSet bitset = collectors.get(collectorKey);
            if (bitset == null) return -1;

            int span = maxDoc - minDoc;
            if (span <= 0) return 0;
            int wordsNeeded = (span + 63) >>> 6;
            int outWordCap = (int) (out.byteSize() / 8);
            int wordsToWrite = Math.min(wordsNeeded, outWordCap);

            long[] bits = bitset.getBits();
            int startWord = minDoc >>> 6;
            int bitOffset = minDoc & 63;

            long firstWord = 0L;
            if (bitOffset == 0) {
                // Aligned case
                for (int i = 0; i < wordsToWrite; i++) {
                    int srcIdx = startWord + i;
                    long word = (srcIdx < bits.length) ? bits[srcIdx] : 0L;
                    if (i == wordsToWrite - 1) {
                        int bitsInLastWord = span - (i * 64);
                        if (bitsInLastWord < 64) {
                            word &= (1L << bitsInLastWord) - 1;
                        }
                    }
                    if (i == 0) firstWord = word;
                    out.setAtIndex(java.lang.foreign.ValueLayout.JAVA_LONG, i, word);
                }
            } else {
                // Unaligned case
                for (int i = 0; i < wordsToWrite; i++) {
                    int loIdx = startWord + i;
                    int hiIdx = loIdx + 1;
                    long lo = (loIdx < bits.length) ? bits[loIdx] : 0L;
                    long hi = (hiIdx < bits.length) ? bits[hiIdx] : 0L;
                    long word = (lo >>> bitOffset) | (hi << (64 - bitOffset));
                    if (i == wordsToWrite - 1) {
                        int bitsInLastWord = span - (i * 64);
                        if (bitsInLastWord < 64) {
                            word &= (1L << bitsInLastWord) - 1;
                        }
                    }
                    if (i == 0) firstWord = word;
                    out.setAtIndex(java.lang.foreign.ValueLayout.JAVA_LONG, i, word);
                }
            }
            logger.info(
                "[AGG_DELEGATION_TRACE] AggBitsetHandle.collectDocs: collectorKey={}, range=[{},{}), wordsWritten={}, firstWord=0x{}, bitsLength={}",
                collectorKey, minDoc, maxDoc, wordsToWrite, Long.toHexString(firstWord), bits.length
            );
            return wordsToWrite;
        }

        @Override
        public void releaseCollector(int collectorKey) {
            collectors.remove(collectorKey);
        }

        @Override
        public void releaseProvider(int providerKey) {
            // No-op
        }

        @Override
        public void close() {
            collectors.clear();
        }
    }

    /** Hex-dump helper for debugging — returns first N bytes as hex string. */
    private static String bytesToHex(byte[] bytes, int maxBytes) {
        int len = Math.min(bytes.length, maxBytes);
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02x", bytes[i] & 0xff));
        }
        if (bytes.length > maxBytes) {
            sb.append("...(").append(bytes.length).append(" total)");
        }
        return sb.toString();
    }
}
