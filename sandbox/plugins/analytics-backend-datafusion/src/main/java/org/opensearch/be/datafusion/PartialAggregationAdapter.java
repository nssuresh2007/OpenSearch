/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Converts Arrow IPC bytes (RecordBatch) returned by the DataFusion Rust runtime into
 * OpenSearch {@link InternalAggregation} subclasses.
 *
 * <p>This adapter is the bridge between DataFusion's Arrow-based output and OpenSearch's
 * {@code InternalAggregation} hierarchy. It:
 * <ol>
 *   <li>Deserializes Arrow IPC bytes into Arrow RecordBatches</li>
 *   <li>Based on the aggregation type, extracts values from the appropriate columns</li>
 *   <li>Constructs the corresponding {@code InternalAggregation} subclass</li>
 *   <li>Preserves metadata (name, {@code meta} map) from the original aggregation request</li>
 * </ol>
 *
 * <h2>Arrow IPC format expectations:</h2>
 * <ul>
 *   <li>For metric aggregations: a single-row RecordBatch with one column per metric value</li>
 *   <li>For bucket aggregations: a multi-row RecordBatch where each row is a bucket, with
 *       columns for bucket key, doc_count, and sub-aggregation values</li>
 * </ul>
 *
 * <h2>Column naming conventions:</h2>
 * <ul>
 *   <li>Metric columns: {@code "value"}, {@code "sum"}, {@code "count"}, {@code "min"},
 *       {@code "max"}, {@code "sum_of_squares"}</li>
 *   <li>Bucket key column: {@code "key"}</li>
 *   <li>Bucket doc_count column: {@code "doc_count"}</li>
 *   <li>Range bucket columns: {@code "key"}, {@code "from"}, {@code "to"}, {@code "doc_count"}</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class PartialAggregationAdapter {

    private static final Logger logger = LogManager.getLogger(PartialAggregationAdapter.class);

    // Column name constants for metric aggregations
    static final String COL_VALUE = "value";
    static final String COL_SUM = "sum";
    static final String COL_COUNT = "count";
    static final String COL_MIN = "min";
    static final String COL_MAX = "max";
    static final String COL_SUM_OF_SQUARES = "sum_of_squares";

    // Column name constants for bucket aggregations
    static final String COL_KEY = "key";
    static final String COL_DOC_COUNT = "doc_count";
    static final String COL_FROM = "from";
    static final String COL_TO = "to";

    private final BufferAllocator allocator;

    /**
     * Creates a new adapter with a default allocator.
     */
    public PartialAggregationAdapter() {
        this(new RootAllocator());
    }

    /**
     * Creates a new adapter with the given allocator.
     *
     * @param allocator the Arrow buffer allocator to use for reading IPC bytes
     */
    public PartialAggregationAdapter(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Converts Arrow IPC bytes into an {@link InternalAggregation} subclass based on the
     * original aggregation type.
     *
     * @param arrowIpcBytes the Arrow IPC stream bytes from DataFusion execution
     * @param originalAgg   the original aggregation builder (provides type, name, metadata)
     * @return the corresponding {@code InternalAggregation} subclass
     * @throws IOException if Arrow IPC deserialization fails
     * @throws IllegalArgumentException if the aggregation type is not supported
     */
    public InternalAggregation convert(byte[] arrowIpcBytes, AggregationBuilder originalAgg) throws IOException {
        String aggType = originalAgg.getType();
        String aggName = originalAgg.getName();
        Map<String, Object> metadata = originalAgg.getMetadata();

        logger.debug("Converting Arrow IPC bytes for aggregation [{}] of type [{}]", aggName, aggType);

        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(arrowIpcBytes), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // Load the first batch
            if (reader.loadNextBatch() == false) {
                // Empty result — return identity values
                return createEmptyResult(aggType, aggName, metadata);
            }

            return switch (aggType) {
                // Metric aggregations
                case "sum" -> convertSum(root, aggName, metadata);
                case "min" -> convertMin(root, aggName, metadata);
                case "max" -> convertMax(root, aggName, metadata);
                case "avg" -> convertAvg(root, aggName, metadata);
                case "value_count", "count" -> convertValueCount(root, aggName, metadata);
                case "stats" -> convertStats(root, aggName, metadata);
                case "extended_stats" -> convertExtendedStats(root, aggName, metadata);

                // Bucket aggregations
                case "terms" -> convertTerms(root, reader, aggName, metadata);
                case "date_histogram" -> convertDateHistogram(root, reader, aggName, metadata);
                case "histogram" -> convertHistogram(root, reader, aggName, metadata);
                case "range" -> convertRange(root, reader, aggName, metadata);
                case "filters" -> convertFilters(root, reader, aggName, metadata);

                // Sketch aggregations (deferred — placeholder)
                case "cardinality" -> throw new UnsupportedOperationException(
                    "Cardinality (HLL) aggregation adapter not yet implemented — deferred to sketch UDAF task"
                );
                case "percentiles" -> throw new UnsupportedOperationException(
                    "Percentiles (T-Digest) aggregation adapter not yet implemented — deferred to sketch UDAF task"
                );

                default -> throw new IllegalArgumentException("Unsupported aggregation type [" + aggType + "] for Arrow IPC conversion");
            };
        }
    }

    // ── Metric Adapters ──────────────────────────────────────────────────────────

    /**
     * Converts a single-row Arrow batch to {@link InternalSum}.
     * Expected column: "value" (Float64)
     */
    InternalSum convertSum(VectorSchemaRoot root, String name, Map<String, Object> metadata) {
        // Debug: log the actual Arrow batch schema and row count
        logger.info(
            "[AGG_DELEGATION_TRACE] convertSum: schema={}, rowCount={}, vectors={}",
            root.getSchema(),
            root.getRowCount(),
            root.getFieldVectors().stream().map(v -> v.getName() + ":" + v.getClass().getSimpleName() + "=" + (v.getValueCount() > 0 ? v.getObject(0) : "empty")).toList()
        );
        double value = getDoubleValue(root, COL_VALUE, 0);
        return new InternalSum(name, value, DocValueFormat.RAW, metadata);
    }

    /**
     * Converts a single-row Arrow batch to {@link InternalMin}.
     * Expected column: "value" (Float64)
     */
    InternalMin convertMin(VectorSchemaRoot root, String name, Map<String, Object> metadata) {
        double value = getDoubleValue(root, COL_VALUE, 0);
        return new InternalMin(name, value, DocValueFormat.RAW, metadata);
    }

    /**
     * Converts a single-row Arrow batch to {@link InternalMax}.
     * Expected column: "value" (Float64)
     */
    InternalMax convertMax(VectorSchemaRoot root, String name, Map<String, Object> metadata) {
        double value = getDoubleValue(root, COL_VALUE, 0);
        return new InternalMax(name, value, DocValueFormat.RAW, metadata);
    }

    /**
     * Converts a single-row Arrow batch to {@link InternalAvg}.
     * Expected columns: "sum" (Float64), "count" (Int64)
     *
     * <p>For partial aggregation state, InternalAvg needs both sum and count
     * (not the final average) so the coordinator can combine partials correctly.
     */
    InternalAvg convertAvg(VectorSchemaRoot root, String name, Map<String, Object> metadata) {
        double sum = getDoubleValue(root, COL_SUM, 0);
        long count = getLongValue(root, COL_COUNT, 0);
        return new InternalAvg(name, sum, count, DocValueFormat.RAW, metadata);
    }

    /**
     * Converts a single-row Arrow batch to {@link InternalValueCount}.
     * Expected column: "value" (Int64) or "count" (Int64)
     */
    InternalValueCount convertValueCount(VectorSchemaRoot root, String name, Map<String, Object> metadata) {
        // Try "value" first, fall back to "count"
        long value;
        if (root.getVector(COL_VALUE) != null) {
            value = getLongValue(root, COL_VALUE, 0);
        } else {
            value = getLongValue(root, COL_COUNT, 0);
        }
        return new InternalValueCount(name, value, metadata);
    }

    /**
     * Converts a single-row Arrow batch to {@link InternalStats}.
     * Expected columns: "count" (Int64), "sum" (Float64), "min" (Float64), "max" (Float64)
     */
    InternalStats convertStats(VectorSchemaRoot root, String name, Map<String, Object> metadata) {
        long count = getLongValue(root, COL_COUNT, 0);
        double sum = getDoubleValue(root, COL_SUM, 0);
        double min = getDoubleValue(root, COL_MIN, 0);
        double max = getDoubleValue(root, COL_MAX, 0);
        return new InternalStats(name, count, sum, min, max, DocValueFormat.RAW, metadata);
    }

    /**
     * Converts a single-row Arrow batch to {@link InternalExtendedStats}.
     * Expected columns: "count" (Int64), "sum" (Float64), "min" (Float64),
     *                   "max" (Float64), "sum_of_squares" (Float64)
     */
    InternalExtendedStats convertExtendedStats(VectorSchemaRoot root, String name, Map<String, Object> metadata) {
        long count = getLongValue(root, COL_COUNT, 0);
        double sum = getDoubleValue(root, COL_SUM, 0);
        double min = getDoubleValue(root, COL_MIN, 0);
        double max = getDoubleValue(root, COL_MAX, 0);
        double sumOfSquares = getDoubleValue(root, COL_SUM_OF_SQUARES, 0);
        // sigma defaults to 2.0 (the standard deviation multiplier for bounds)
        double sigma = 2.0;
        return new InternalExtendedStats(name, count, sum, min, max, sumOfSquares, sigma, DocValueFormat.RAW, metadata);
    }

    // ── Bucket Adapters ──────────────────────────────────────────────────────────

    /**
     * Converts a multi-row Arrow batch to a terms aggregation.
     * Dispatches to {@link StringTerms}, {@link LongTerms}, or {@link DoubleTerms}
     * based on the Arrow column type of the "key" column.
     *
     * <p>Expected columns: "key" (Utf8/Int64/Float64), "doc_count" (Int64)
     */
    InternalAggregation convertTerms(VectorSchemaRoot root, ArrowStreamReader reader, String name, Map<String, Object> metadata)
        throws IOException {
        Schema schema = root.getSchema();
        var keyField = schema.findField(COL_KEY);
        if (keyField == null) {
            throw new IllegalStateException("Terms aggregation Arrow batch missing 'key' column");
        }

        String arrowTypeName = keyField.getType().getTypeID().name();
        return switch (arrowTypeName) {
            case "Utf8" -> convertStringTerms(root, reader, name, metadata);
            case "Int" -> convertLongTerms(root, reader, name, metadata);
            case "FloatingPoint" -> convertDoubleTerms(root, reader, name, metadata);
            default -> throw new IllegalStateException("Unsupported Arrow type [" + arrowTypeName + "] for terms aggregation key column");
        };
    }

    /**
     * Converts Arrow batch rows to {@link StringTerms}.
     */
    private StringTerms convertStringTerms(VectorSchemaRoot root, ArrowStreamReader reader, String name, Map<String, Object> metadata)
        throws IOException {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        do {
            int rowCount = root.getRowCount();
            VarCharVector keyVector = (VarCharVector) root.getVector(COL_KEY);
            BigIntVector docCountVector = (BigIntVector) root.getVector(COL_DOC_COUNT);

            for (int i = 0; i < rowCount; i++) {
                byte[] keyBytes = keyVector.get(i);
                BytesRef term = new BytesRef(keyBytes);
                long docCount = docCountVector.get(i);
                buckets.add(new StringTerms.Bucket(term, docCount, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
            }
        } while (reader.loadNextBatch());

        return new StringTerms(
            name,
            BucketOrder.count(false),  // reduceOrder: by count descending
            BucketOrder.count(false),  // order: by count descending (default)
            metadata,
            DocValueFormat.RAW,
            buckets.size(),            // shardSize
            false,                     // showTermDocCountError
            0,                         // otherDocCount
            buckets,
            0,                         // docCountError
            new TermsAggregator.BucketCountThresholds(1, 1, buckets.size(), buckets.size())
        );
    }

    /**
     * Converts Arrow batch rows to {@link LongTerms}.
     */
    private LongTerms convertLongTerms(VectorSchemaRoot root, ArrowStreamReader reader, String name, Map<String, Object> metadata)
        throws IOException {
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        do {
            int rowCount = root.getRowCount();
            BigIntVector keyVector = (BigIntVector) root.getVector(COL_KEY);
            BigIntVector docCountVector = (BigIntVector) root.getVector(COL_DOC_COUNT);

            for (int i = 0; i < rowCount; i++) {
                long key = keyVector.get(i);
                long docCount = docCountVector.get(i);
                buckets.add(new LongTerms.Bucket(key, docCount, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
            }
        } while (reader.loadNextBatch());

        return new LongTerms(
            name,
            BucketOrder.count(false),
            BucketOrder.count(false),
            metadata,
            DocValueFormat.RAW,
            buckets.size(),
            false,
            0,
            buckets,
            0,
            new TermsAggregator.BucketCountThresholds(1, 1, buckets.size(), buckets.size())
        );
    }

    /**
     * Converts Arrow batch rows to {@link DoubleTerms}.
     */
    private DoubleTerms convertDoubleTerms(VectorSchemaRoot root, ArrowStreamReader reader, String name, Map<String, Object> metadata)
        throws IOException {
        List<DoubleTerms.Bucket> buckets = new ArrayList<>();
        do {
            int rowCount = root.getRowCount();
            Float8Vector keyVector = (Float8Vector) root.getVector(COL_KEY);
            BigIntVector docCountVector = (BigIntVector) root.getVector(COL_DOC_COUNT);

            for (int i = 0; i < rowCount; i++) {
                double key = keyVector.get(i);
                long docCount = docCountVector.get(i);
                buckets.add(new DoubleTerms.Bucket(key, docCount, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW));
            }
        } while (reader.loadNextBatch());

        return new DoubleTerms(
            name,
            BucketOrder.count(false),
            BucketOrder.count(false),
            metadata,
            DocValueFormat.RAW,
            buckets.size(),
            false,
            0,
            buckets,
            0,
            new TermsAggregator.BucketCountThresholds(1, 1, buckets.size(), buckets.size())
        );
    }

    /**
     * Converts a multi-row Arrow batch to {@link InternalDateHistogram}.
     * Expected columns: "key" (Int64 epoch millis), "doc_count" (Int64)
     */
    InternalAggregation convertDateHistogram(VectorSchemaRoot root, ArrowStreamReader reader, String name, Map<String, Object> metadata)
        throws IOException {
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>();
        do {
            int rowCount = root.getRowCount();
            BigIntVector keyVector = (BigIntVector) root.getVector(COL_KEY);
            BigIntVector docCountVector = (BigIntVector) root.getVector(COL_DOC_COUNT);

            for (int i = 0; i < rowCount; i++) {
                long key = keyVector.get(i);
                long docCount = docCountVector.get(i);
                buckets.add(new InternalDateHistogram.Bucket(key, docCount, false, DocValueFormat.RAW, InternalAggregations.EMPTY));
            }
        } while (reader.loadNextBatch());

        return createInternalDateHistogram(name, buckets, metadata);
    }

    /**
     * Creates an {@link InternalDateHistogram} instance via reflection because the constructor
     * is package-private. This is acceptable for a sandbox plugin that constructs partial
     * aggregation state from a backend execution result.
     */
    @SuppressWarnings("unchecked")
    private static InternalDateHistogram createInternalDateHistogram(
        String name,
        List<InternalDateHistogram.Bucket> buckets,
        Map<String, Object> metadata
    ) {
        try {
            // The InternalDateHistogram constructor is package-private; use reflection.
            // Constructor signature: (String, List, BucketOrder, long, long, EmptyBucketInfo, DocValueFormat, boolean, Map)
            Constructor<?>[] ctors = InternalDateHistogram.class.getDeclaredConstructors();
            Constructor<?> targetCtor = null;
            for (Constructor<?> ctor : ctors) {
                Class<?>[] params = ctor.getParameterTypes();
                if (params.length == 9 && params[0] == String.class && params[1] == List.class) {
                    targetCtor = ctor;
                    break;
                }
            }
            if (targetCtor == null) {
                throw new IllegalStateException("Cannot find InternalDateHistogram constructor with 9 parameters");
            }
            targetCtor.setAccessible(true);
            return (InternalDateHistogram) targetCtor.newInstance(
                name,
                buckets,
                BucketOrder.key(true),  // date histograms are ordered by key ascending
                1L,                     // minDocCount
                0L,                     // offset
                null,                   // emptyBucketInfo (null for minDocCount > 0)
                DocValueFormat.RAW,
                false,                  // keyed
                metadata
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to construct InternalDateHistogram via reflection", e);
        }
    }

    /**
     * Converts a multi-row Arrow batch to {@link InternalHistogram}.
     * Expected columns: "key" (Float64), "doc_count" (Int64)
     */
    InternalAggregation convertHistogram(VectorSchemaRoot root, ArrowStreamReader reader, String name, Map<String, Object> metadata)
        throws IOException {
        List<InternalHistogram.Bucket> buckets = new ArrayList<>();
        do {
            int rowCount = root.getRowCount();
            Float8Vector keyVector = (Float8Vector) root.getVector(COL_KEY);
            BigIntVector docCountVector = (BigIntVector) root.getVector(COL_DOC_COUNT);

            for (int i = 0; i < rowCount; i++) {
                double key = keyVector.get(i);
                long docCount = docCountVector.get(i);
                buckets.add(new InternalHistogram.Bucket(key, docCount, false, DocValueFormat.RAW, InternalAggregations.EMPTY));
            }
        } while (reader.loadNextBatch());

        return new InternalHistogram(
            name,
            buckets,
            BucketOrder.key(true),  // histograms are ordered by key ascending
            1,                      // minDocCount
            null,                   // emptyBucketInfo (null for minDocCount > 0)
            DocValueFormat.RAW,
            false,                  // keyed
            metadata
        );
    }

    /**
     * Converts a multi-row Arrow batch to {@link InternalRange}.
     * Expected columns: "key" (Utf8), "from" (Float64), "to" (Float64), "doc_count" (Int64)
     */
    @SuppressWarnings("unchecked")
    InternalAggregation convertRange(VectorSchemaRoot root, ArrowStreamReader reader, String name, Map<String, Object> metadata)
        throws IOException {
        List<InternalRange.Bucket> buckets = new ArrayList<>();
        do {
            int rowCount = root.getRowCount();
            VarCharVector keyVector = (VarCharVector) root.getVector(COL_KEY);
            Float8Vector fromVector = (Float8Vector) root.getVector(COL_FROM);
            Float8Vector toVector = (Float8Vector) root.getVector(COL_TO);
            BigIntVector docCountVector = (BigIntVector) root.getVector(COL_DOC_COUNT);

            for (int i = 0; i < rowCount; i++) {
                String key = new String(keyVector.get(i));
                double from = fromVector.isNull(i) ? Double.NEGATIVE_INFINITY : fromVector.get(i);
                double to = toVector.isNull(i) ? Double.POSITIVE_INFINITY : toVector.get(i);
                long docCount = docCountVector.get(i);
                buckets.add(new InternalRange.Bucket(key, from, to, docCount, InternalAggregations.EMPTY, false, DocValueFormat.RAW));
            }
        } while (reader.loadNextBatch());

        return new InternalRange<>(name, buckets, DocValueFormat.RAW, false, metadata);
    }

    /**
     * Converts a multi-row Arrow batch to {@link InternalFilters}.
     * Expected columns: "key" (Utf8), "doc_count" (Int64)
     */
    InternalAggregation convertFilters(VectorSchemaRoot root, ArrowStreamReader reader, String name, Map<String, Object> metadata)
        throws IOException {
        List<InternalFilters.InternalBucket> buckets = new ArrayList<>();
        do {
            int rowCount = root.getRowCount();
            VarCharVector keyVector = (VarCharVector) root.getVector(COL_KEY);
            BigIntVector docCountVector = (BigIntVector) root.getVector(COL_DOC_COUNT);

            for (int i = 0; i < rowCount; i++) {
                String key = new String(keyVector.get(i));
                long docCount = docCountVector.get(i);
                buckets.add(new InternalFilters.InternalBucket(key, docCount, InternalAggregations.EMPTY, true));
            }
        } while (reader.loadNextBatch());

        return new InternalFilters(name, buckets, true, metadata);
    }

    // ── Empty result helpers ─────────────────────────────────────────────────────

    /**
     * Creates an identity/empty result for the given aggregation type when the Arrow batch
     * is empty (no matching documents).
     */
    private InternalAggregation createEmptyResult(String aggType, String name, Map<String, Object> metadata) {
        return switch (aggType) {
            case "sum" -> new InternalSum(name, 0.0, DocValueFormat.RAW, metadata);
            case "min" -> new InternalMin(name, Double.POSITIVE_INFINITY, DocValueFormat.RAW, metadata);
            case "max" -> new InternalMax(name, Double.NEGATIVE_INFINITY, DocValueFormat.RAW, metadata);
            case "avg" -> new InternalAvg(name, 0.0, 0, DocValueFormat.RAW, metadata);
            case "value_count", "count" -> new InternalValueCount(name, 0, metadata);
            case "stats" -> new InternalStats(
                name,
                0,
                0.0,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                DocValueFormat.RAW,
                metadata
            );
            case "extended_stats" -> new InternalExtendedStats(
                name,
                0,
                0.0,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                0.0,
                2.0,
                DocValueFormat.RAW,
                metadata
            );
            case "terms" -> new LongTerms(
                name,
                BucketOrder.count(false),
                BucketOrder.count(false),
                metadata,
                DocValueFormat.RAW,
                0,
                false,
                0,
                Collections.emptyList(),
                0,
                new TermsAggregator.BucketCountThresholds(1, 1, 10, 10)
            );
            case "date_histogram" -> createInternalDateHistogram(name, Collections.emptyList(), metadata);
            case "histogram" -> new InternalHistogram(
                name,
                Collections.emptyList(),
                BucketOrder.key(true),
                1,
                null,
                DocValueFormat.RAW,
                false,
                metadata
            );
            case "range" -> new InternalRange<>(name, Collections.emptyList(), DocValueFormat.RAW, false, metadata);
            case "filters" -> new InternalFilters(name, Collections.emptyList(), true, metadata);
            default -> throw new IllegalArgumentException("Unsupported aggregation type [" + aggType + "] for empty result creation");
        };
    }

    // ── Column value extraction helpers ──────────────────────────────────────────

    /**
     * Extracts a double value from the named column at the given row index.
     * Falls back to 0.0 if the column is null or the value is null.
     */
    private static double getDoubleValue(VectorSchemaRoot root, String columnName, int rowIndex) {
        var vector = root.getVector(columnName);
        if (vector == null) {
            logger.warn("Expected column [{}] not found in Arrow batch; returning 0.0", columnName);
            return 0.0;
        }
        if (vector.isNull(rowIndex)) {
            return 0.0;
        }
        if (vector instanceof Float8Vector float8) {
            return float8.get(rowIndex);
        }
        if (vector instanceof BigIntVector bigInt) {
            return (double) bigInt.get(rowIndex);
        }
        // Generic fallback via Object
        Object obj = vector.getObject(rowIndex);
        if (obj instanceof Number num) {
            return num.doubleValue();
        }
        logger.warn("Cannot extract double from column [{}] of type [{}]; returning 0.0", columnName, vector.getClass().getSimpleName());
        return 0.0;
    }

    /**
     * Extracts a long value from the named column at the given row index.
     * Falls back to 0 if the column is null or the value is null.
     */
    private static long getLongValue(VectorSchemaRoot root, String columnName, int rowIndex) {
        var vector = root.getVector(columnName);
        if (vector == null) {
            logger.warn("Expected column [{}] not found in Arrow batch; returning 0", columnName);
            return 0;
        }
        if (vector.isNull(rowIndex)) {
            return 0;
        }
        if (vector instanceof BigIntVector bigInt) {
            return bigInt.get(rowIndex);
        }
        if (vector instanceof Float8Vector float8) {
            return (long) float8.get(rowIndex);
        }
        // Generic fallback via Object
        Object obj = vector.getObject(rowIndex);
        if (obj instanceof Number num) {
            return num.longValue();
        }
        logger.warn("Cannot extract long from column [{}] of type [{}]; returning 0", columnName, vector.getClass().getSimpleName());
        return 0;
    }

    /**
     * Releases the allocator resources. Should be called when the adapter is no longer needed.
     */
    public void close() {
        allocator.close();
    }
}
