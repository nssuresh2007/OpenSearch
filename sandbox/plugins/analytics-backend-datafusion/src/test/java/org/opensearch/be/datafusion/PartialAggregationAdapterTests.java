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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

/**
 * Unit tests for {@link PartialAggregationAdapter} — verifies that synthetic Arrow IPC
 * batches are correctly converted to OpenSearch {@link InternalAggregation} subclasses.
 *
 * <p>This is part of the aggregation delegation smoke test (Task 9) verifying that the
 * adapter can convert a synthetic Arrow IPC batch to {@link InternalSum}.
 */
public class PartialAggregationAdapterTests extends OpenSearchTestCase {

    /**
     * Verifies that {@link PartialAggregationAdapter} can convert a synthetic Arrow IPC
     * batch containing a sum value into an {@link InternalSum}.
     */
    public void testConvertSumFromArrowIpc() throws IOException {
        // Arrange: create a synthetic Arrow IPC batch with a "value" column containing 150.0
        byte[] arrowIpcBytes = createSumArrowIpcBytes(150.0);
        AggregationBuilder sumAgg = AggregationBuilders.sum("total_revenue").field("revenue");

        // Act: convert the Arrow IPC bytes to InternalSum
        try (BufferAllocator allocator = new RootAllocator()) {
            PartialAggregationAdapter adapter = new PartialAggregationAdapter(allocator);
            InternalAggregation result = adapter.convert(arrowIpcBytes, sumAgg);

            // Assert
            assertNotNull("Result should not be null", result);
            assertTrue("Result should be InternalSum", result instanceof InternalSum);
            InternalSum sum = (InternalSum) result;
            assertEquals("Sum name should match", "total_revenue", sum.getName());
            assertEquals("Sum value should be 150.0", 150.0, sum.getValue(), 0.001);
        }
    }

    /**
     * Verifies that {@link PartialAggregationAdapter} returns an identity (zero) sum
     * when the Arrow IPC batch is empty (no rows).
     */
    public void testConvertEmptyBatchReturnsIdentitySum() throws IOException {
        // Arrange: create an empty Arrow IPC batch (schema only, no batches)
        byte[] emptyArrowIpcBytes = createEmptyArrowIpcBytes();
        AggregationBuilder sumAgg = AggregationBuilders.sum("total_revenue").field("revenue");

        // Act
        try (BufferAllocator allocator = new RootAllocator()) {
            PartialAggregationAdapter adapter = new PartialAggregationAdapter(allocator);
            InternalAggregation result = adapter.convert(emptyArrowIpcBytes, sumAgg);

            // Assert: empty batch should produce identity sum (0.0)
            assertNotNull("Result should not be null", result);
            assertTrue("Result should be InternalSum", result instanceof InternalSum);
            InternalSum sum = (InternalSum) result;
            assertEquals("Empty sum should be 0.0", 0.0, sum.getValue(), 0.001);
        }
    }

    /**
     * Verifies that the adapter correctly handles a negative sum value.
     */
    public void testConvertNegativeSumValue() throws IOException {
        byte[] arrowIpcBytes = createSumArrowIpcBytes(-42.5);
        AggregationBuilder sumAgg = AggregationBuilders.sum("negative_sum").field("balance");

        try (BufferAllocator allocator = new RootAllocator()) {
            PartialAggregationAdapter adapter = new PartialAggregationAdapter(allocator);
            InternalAggregation result = adapter.convert(arrowIpcBytes, sumAgg);

            assertNotNull("Result should not be null", result);
            assertTrue("Result should be InternalSum", result instanceof InternalSum);
            InternalSum sum = (InternalSum) result;
            assertEquals("Sum name should match", "negative_sum", sum.getName());
            assertEquals("Sum value should be -42.5", -42.5, sum.getValue(), 0.001);
        }
    }

    // ── Helper methods ──────────────────────────────────────────────────────────

    /**
     * Creates a synthetic Arrow IPC stream containing a single-row batch with a "value"
     * column holding the given sum value.
     */
    private byte[] createSumArrowIpcBytes(double sumValue) throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(
                List.of(new Field("value", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))
            );

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                Float8Vector valueVector = (Float8Vector) root.getVector("value");
                valueVector.allocateNew(1);
                valueVector.set(0, sumValue);
                valueVector.setValueCount(1);
                root.setRowCount(1);

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                return out.toByteArray();
            }
        }
    }

    /**
     * Creates an empty Arrow IPC stream (schema only, no batches).
     */
    private byte[] createEmptyArrowIpcBytes() throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(
                List.of(new Field("value", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))
            );

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.setRowCount(0);

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                    writer.start();
                    // Don't write any batch — just end the stream
                    writer.end();
                }
                return out.toByteArray();
            }
        }
    }
}
