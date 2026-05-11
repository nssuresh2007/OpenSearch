/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reusable utilities for extracting fields and values from PPL relevance function
 * RexCall structures and serializing QueryBuilders.
 *
 * <p>PPL relevance functions encode arguments as MAP_VALUE_CONSTRUCTOR pairs:
 * {@code func(MAP('field', $ref), MAP('query', literal), [MAP('param', literal)]...)}
 * Each MAP has exactly 2 operands: key at index 0, value at index 1.
 */
final class ConversionUtils {

    private ConversionUtils() {}

    /**
     * Result of extracting operands from a standard comparison RexCall.
     */
    record ComparisonOperands(String fieldName, Object value) {
    }

    /**
     * Result of extracting operands from an IN predicate RexCall.
     */
    record InOperands(String fieldName, List<Object> values) {
    }

    /**
     * Extracts field name and literal value from a standard comparison RexCall
     * (EQUALS, GT, GTE, LT, LTE, etc.).
     *
     * <p>Standard comparisons have exactly 2 operands: one RexInputRef (field)
     * and one RexLiteral (value). The order may be either [field, literal] or
     * [literal, field] depending on Calcite optimization passes.
     *
     * <p>Example: for PPL {@code where age = 25}, Calcite produces
     * {@code RexCall(EQUALS, [RexInputRef(1), RexLiteral(25:INTEGER)])} where
     * index 1 resolves to field name "age" via fieldStorage. The method returns
     * {@code ComparisonOperands("age", 25)}.
     *
     * @param call         the comparison RexCall
     * @param fieldStorage per-column storage metadata for resolving field names
     * @return extracted field name and typed value
     * @throws IllegalArgumentException if operands are not one RexInputRef + one RexLiteral
     */
    static ComparisonOperands extractComparisonOperands(RexCall call, List<FieldStorageInfo> fieldStorage) {
        RexNode op0 = call.getOperands().get(0);
        RexNode op1 = call.getOperands().get(1);

        RexInputRef inputRef;
        RexLiteral literal;

        if (op0 instanceof RexInputRef ref && op1 instanceof RexLiteral lit) {
            inputRef = ref;
            literal = lit;
        } else if (op0 instanceof RexLiteral lit && op1 instanceof RexInputRef ref) {
            inputRef = ref;
            literal = lit;
        } else {
            throw new IllegalArgumentException(
                "Expected one RexInputRef and one RexLiteral, got: ["
                    + op0.getClass().getSimpleName()
                    + ", "
                    + op1.getClass().getSimpleName()
                    + "]"
            );
        }

        String fieldName = FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
        Object value = extractTypedValue(literal);
        return new ComparisonOperands(fieldName, value);
    }

    /**
     * Extracts field name and literal values from an IN predicate RexCall.
     *
     * <p>IN predicates have N+1 operands: one RexInputRef (field) at index 0,
     * followed by N RexLiteral values at indices 1..N. Unlike binary comparisons,
     * the operand order is fixed (field is always first).
     *
     * <p>Example: for PPL {@code where status IN ('active', 'pending')}, Calcite produces
     * {@code RexCall(IN, [RexInputRef(0), RexLiteral('active'), RexLiteral('pending')])}
     * where index 0 resolves to field name "status" via fieldStorage. The method returns
     * {@code InOperands("status", List.of("active", "pending"))}.
     *
     * @param call         the IN RexCall
     * @param fieldStorage per-column storage metadata for resolving field names
     * @return extracted field name and list of typed values
     * @throws IllegalArgumentException if first operand is not RexInputRef,
     *         if any subsequent operand is not RexLiteral, or if fewer than 2 operands exist
     */
    static InOperands extractInOperands(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() < 2) {
            throw new IllegalArgumentException("IN predicate requires at least 2 operands (field + 1 value), got: " + operands.size());
        }

        RexNode firstOperand = operands.get(0);
        if (!(firstOperand instanceof RexInputRef inputRef)) {
            throw new IllegalArgumentException("First operand of IN must be RexInputRef, got: " + firstOperand.getClass().getSimpleName());
        }

        String fieldName = FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
        List<Object> values = new ArrayList<>(operands.size() - 1);

        for (int i = 1; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (!(operand instanceof RexLiteral literal)) {
                throw new IllegalArgumentException(
                    "Operand at index " + i + " of IN must be RexLiteral, got: " + operand.getClass().getSimpleName()
                );
            }
            values.add(extractTypedValue(literal));
        }

        return new InOperands(fieldName, values);
    }

    /**
     * Extracts a typed Java value from a RexLiteral based on its SQL type.
     *
     * <ul>
     *   <li>VARCHAR/CHAR → String</li>
     *   <li>INTEGER → Integer</li>
     *   <li>BIGINT → Long</li>
     *   <li>FLOAT/REAL → Float</li>
     *   <li>DOUBLE → Double</li>
     *   <li>BOOLEAN → Boolean</li>
     *   <li>TIMESTAMP/DATE → Long (epoch millis)</li>
     * </ul>
     *
     * @throws IllegalArgumentException for unsupported types (e.g., ARRAY, MAP)
     */
    private static Object extractTypedValue(RexLiteral literal) {
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        switch (typeName) {
            case VARCHAR:
            case CHAR:
                return literal.getValueAs(String.class);
            case INTEGER:
                return literal.getValueAs(Integer.class);
            case BIGINT:
                return literal.getValueAs(Long.class);
            case FLOAT:
            case REAL:
                return literal.getValueAs(Float.class);
            case DOUBLE:
                return literal.getValueAs(Double.class);
            case BOOLEAN:
                return literal.getValueAs(Boolean.class);
            case TIMESTAMP:
            case DATE:
                return literal.getValueAs(Long.class);
            default:
                throw new IllegalArgumentException("Unsupported literal type: " + typeName);
        }
    }

    /**
     * Extracts field name from a MAP_VALUE_CONSTRUCTOR operand: MAP('field', $inputRef).
     */
    static String extractFieldFromRelevanceMap(RexCall call, int operandIndex, List<FieldStorageInfo> fieldStorage) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (operand instanceof RexCall mapCall) {
            RexNode value = mapCall.getOperands().get(1);
            if (value instanceof RexInputRef inputRef) {
                return FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
            }
        }
        if (operand instanceof RexInputRef inputRef) {
            return FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
        }
        throw new IllegalArgumentException("Cannot extract field name from operand " + operandIndex + ": " + operand);
    }

    /**
     * Extracts string value from a MAP_VALUE_CONSTRUCTOR operand: MAP('key', 'value').
     */
    static String extractStringFromRelevanceMap(RexCall call, int operandIndex) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (operand instanceof RexCall mapCall) {
            RexNode value = mapCall.getOperands().get(1);
            if (value instanceof RexLiteral literal) {
                return literal.getValueAs(String.class);
            }
        }
        if (operand instanceof RexLiteral literal) {
            return literal.getValueAs(String.class);
        }
        throw new IllegalArgumentException("Cannot extract string from operand " + operandIndex + ": " + operand);
    }

    /**
     * Serializes a QueryBuilder into bytes using NamedWriteable protocol.
     */
    static byte[] serializeQueryBuilder(QueryBuilder queryBuilder) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(queryBuilder);
            return BytesReference.toBytes(output.bytes());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to serialize delegated query: " + queryBuilder, exception);
        }
    }

    /**
     * Extracts multiple field names from a MAP_VALUE_CONSTRUCTOR operand
     * for multi-field full-text functions (multi_match, query_string, simple_query_string).
     *
     * <p>The operand structure for multi-field functions:
     * {@code MAP('fields', MAP('field1':VARCHAR, boost1:DOUBLE, 'field2':VARCHAR, boost2:DOUBLE, ...))}
     * The outer MAP has key='fields' at index 0 and a nested MAP at index 1.
     * The nested MAP is a Calcite MAP_VALUE_CONSTRUCTOR with strict alternating key-value pairs:
     * field name (VARCHAR) at even indices, boost value (DOUBLE) at odd indices.
     *
     * <p>Also supports the RexInputRef-based structure for single-field fallback:
     * {@code MAP('field', $ref1, 'field', $ref2, ...)}
     *
     * <p>Note: This method is intentionally not recursive. The MAP nesting depth is bounded
     * to at most 2 levels by Calcite's MAP_VALUE_CONSTRUCTOR design: an outer MAP holding
     * the 'fields' key and a nested MAP holding field-name/boost pairs. Deeper nesting does
     * not occur in the PPL relevance function encoding.
     *
     * <p>TODO: extract per-field boost values and return them alongside field names.
     */
    static List<String> extractFieldsFromRelevanceMap(RexCall call, int operandIndex, List<FieldStorageInfo> fieldStorage) {
        RexNode operand = call.getOperands().get(operandIndex);
        List<String> fields = new ArrayList<>();
        if (operand instanceof RexCall outerMapCall) {
            // Check if the value (index 1) is a nested MAP containing field name/boost pairs
            if (outerMapCall.getOperands().size() >= 2) {
                RexNode value = outerMapCall.getOperands().get(1);
                if (value instanceof RexCall nestedMapCall) {
                    // Nested MAP: strict alternating key-value pairs from MAP_VALUE_CONSTRUCTOR.
                    // Even indices (0, 2, 4...) are field name VARCHAR literals.
                    // Odd indices (1, 3, 5...) are boost DOUBLE literals (ignored for now).
                    List<RexNode> nestedOperands = nestedMapCall.getOperands();
                    for (int i = 0; i < nestedOperands.size(); i += 2) {
                        RexNode fieldNode = nestedOperands.get(i);
                        if (fieldNode instanceof RexLiteral fieldLiteral) {
                            fields.add(fieldLiteral.getValueAs(String.class));
                        }
                    }
                    if (fields.isEmpty() == false) {
                        return fields;
                    }
                }
            }
            // Fallback: RexInputRef-based structure MAP('field', $ref1, 'field', $ref2, ...)
            List<RexNode> mapOperands = outerMapCall.getOperands();
            for (int i = 1; i < mapOperands.size(); i += 2) {
                RexNode val = mapOperands.get(i);
                if (val instanceof RexInputRef inputRef) {
                    fields.add(FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName());
                }
            }
        }
        if (operand instanceof RexInputRef inputRef) {
            fields.add(FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Cannot extract field names from operand " + operandIndex + ": " + operand);
        }
        return fields;
    }
}
