/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.be.lucene.ConversionUtils.ComparisonOperands;
import org.opensearch.be.lucene.ConversionUtils.InOperands;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link ConversionUtils#extractFieldsFromRelevanceMap}.
 */
public class ConversionUtilsTests extends OpenSearchTestCase {

    private static final SqlFunction MULTI_MATCH_FUNCTION = new SqlFunction(
        "MULTI_MATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    /**
     * Tests that extractFieldsFromRelevanceMap returns a single-element list when the
     * nested MAP operand contains exactly one field name/boost pair.
     * Validates Requirement 9.2.
     */
    public void testSingleFieldExtractionReturnsOneElementList() {
        // Structure: MULTI_MATCH(MAP('fields', MAP('title', 1.0)), MAP('query', 'hello'))
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        // Inner nested MAP: field name literal + boost literal (boost ignored, only field name extracted)
        RexNode fieldNameLiteral = rexBuilder.makeLiteral("title");
        RexNode boostLiteral = rexBuilder.makeExactLiteral(new BigDecimal("1.0"), doubleType);
        RexNode nestedMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, fieldNameLiteral, boostLiteral);

        // Outer MAP: MAP('fields', nestedMap)
        RexNode fieldsKeyLiteral = rexBuilder.makeLiteral("fields");
        RexNode outerMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, fieldsKeyLiteral, nestedMap);

        // Query MAP: MAP('query', 'hello')
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello")
        );

        // Top-level call: MULTI_MATCH(outerMap, queryMap)
        RexCall topCall = (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, outerMap, queryMap);

        // FieldStorageInfo list is not used in the nested MAP path (literal field names)
        List<FieldStorageInfo> fieldStorage = List.of();

        List<String> result = ConversionUtils.extractFieldsFromRelevanceMap(topCall, 0, fieldStorage);

        assertEquals("Should return exactly one field", 1, result.size());
        assertEquals("title", result.get(0));
    }

    /**
     * Tests that extractFieldsFromRelevanceMap returns all fields in order when the
     * nested MAP operand contains multiple field name/boost pairs.
     * Validates Requirement 9.1.
     */
    public void testMultiFieldExtractionReturnsAllFieldsInOrder() {
        // Structure: MAP('fields', MAP('title', 2.0, 'body', 1.0, 'tags', 0.5))
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        RexNode titleLiteral = rexBuilder.makeLiteral("title");
        RexNode titleBoost = rexBuilder.makeExactLiteral(new BigDecimal("2.0"), doubleType);
        RexNode bodyLiteral = rexBuilder.makeLiteral("body");
        RexNode bodyBoost = rexBuilder.makeExactLiteral(new BigDecimal("1.0"), doubleType);
        RexNode tagsLiteral = rexBuilder.makeLiteral("tags");
        RexNode tagsBoost = rexBuilder.makeExactLiteral(new BigDecimal("0.5"), doubleType);

        // Nested MAP with 3 field/boost pairs
        RexNode nestedMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            titleLiteral,
            titleBoost,
            bodyLiteral,
            bodyBoost,
            tagsLiteral,
            tagsBoost
        );

        // Outer MAP: MAP('fields', nestedMap)
        RexNode outerMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral("fields"), nestedMap);

        // Query MAP
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("search text")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, outerMap, queryMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        List<String> result = ConversionUtils.extractFieldsFromRelevanceMap(topCall, 0, fieldStorage);

        assertEquals("Should return exactly three fields", 3, result.size());
        assertEquals("title", result.get(0));
        assertEquals("body", result.get(1));
        assertEquals("tags", result.get(2));
    }

    /**
     * Tests that extractFieldsFromRelevanceMap works with the RexInputRef fallback path
     * when the operand uses the MAP('field', $ref1, 'field', $ref2, ...) structure.
     * Validates Requirement 9.1 (fallback path).
     */
    public void testMultiFieldExtractionWithRexInputRefFallback() {
        // Structure: MAP('field', $0, 'field', $1) — RexInputRef-based multi-field
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode key1 = rexBuilder.makeLiteral("field");
        RexNode ref0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode key2 = rexBuilder.makeLiteral("field");
        RexNode ref1 = rexBuilder.makeInputRef(varcharType, 1);

        RexNode fieldMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, key1, ref0, key2, ref1);

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test query")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, fieldMap, queryMap);

        // FieldStorageInfo list maps index 0 → "title", index 1 → "body"
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false),
            new FieldStorageInfo("body", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        List<String> result = ConversionUtils.extractFieldsFromRelevanceMap(topCall, 0, fieldStorage);

        assertEquals("Should return exactly two fields", 2, result.size());
        assertEquals("title", result.get(0));
        assertEquals("body", result.get(1));
    }

    /**
     * Tests that extractFieldsFromRelevanceMap throws IllegalArgumentException when the
     * operand contains no resolvable field references.
     * Validates Requirement 9.3.
     */
    public void testNoFieldsThrowsIllegalArgumentException() {
        // Structure: MAP('fields', 'not_a_map') — value is a literal, not a nested RexCall
        RexNode outerMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("fields"),
            rexBuilder.makeLiteral("not_a_map")
        );

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test")
        );

        RexCall topCall = (RexCall) rexBuilder.makeCall(MULTI_MATCH_FUNCTION, outerMap, queryMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ConversionUtils.extractFieldsFromRelevanceMap(topCall, 0, fieldStorage)
        );
        assertTrue(
            "Exception message should mention operand index",
            exception.getMessage().contains("Cannot extract field names from operand 0")
        );
    }

    // ===== Edge case tests for extractComparisonOperands (Task 1.4) =====

    /**
     * Tests that extractComparisonOperands throws IllegalArgumentException when both
     * operands are RexLiteral (no field reference present).
     * Validates Requirements 4.8, 7.1.
     */
    public void testExtractComparisonOperandsThrowsForTwoLiterals() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode literal1 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(42), intType);
        RexNode literal2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(99), intType);

        RexCall equalsCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, literal1, literal2);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("status", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        expectThrows(IllegalArgumentException.class, () -> ConversionUtils.extractComparisonOperands(equalsCall, fieldStorage));
    }

    /**
     * Tests that extractComparisonOperands throws IllegalArgumentException when both
     * operands are RexInputRef (no literal value present).
     * Validates Requirements 4.8, 7.1.
     */
    public void testExtractComparisonOperandsThrowsForTwoInputRefs() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode ref0 = rexBuilder.makeInputRef(intType, 0);
        RexNode ref1 = rexBuilder.makeInputRef(intType, 1);

        RexCall equalsCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, ref1);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("age", "integer", FieldType.INTEGER, List.of(), List.of("lucene"), List.of(), false),
            new FieldStorageInfo("count", "integer", FieldType.INTEGER, List.of(), List.of("lucene"), List.of(), false)
        );

        expectThrows(IllegalArgumentException.class, () -> ConversionUtils.extractComparisonOperands(equalsCall, fieldStorage));
    }

    /**
     * Tests that extractComparisonOperands throws IllegalArgumentException when the
     * RexLiteral has an unsupported type (ARRAY).
     * Validates Requirements 4.8, 7.1.
     */
    public void testExtractComparisonOperandsThrowsForUnsupportedArrayType() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType arrayType = typeFactory.createArrayType(intType, -1);

        RexNode inputRef = rexBuilder.makeInputRef(intType, 0);
        RexNode arrayLiteral = rexBuilder.makeNullLiteral(arrayType);

        RexCall equalsCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, inputRef, arrayLiteral);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("tags", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        expectThrows(IllegalArgumentException.class, () -> ConversionUtils.extractComparisonOperands(equalsCall, fieldStorage));
    }

    /**
     * Tests that extractComparisonOperands correctly extracts a Boolean value as a
     * Boolean object (not as a String like "true"/"false").
     * Validates Requirement 4.6 (BOOLEAN → Boolean).
     */
    public void testExtractComparisonOperandsBooleanProducesBoolean() {
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RexNode inputRef = rexBuilder.makeInputRef(intType, 0);
        RexNode boolLiteral = rexBuilder.makeLiteral(true, boolType);

        RexCall equalsCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, inputRef, boolLiteral);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("active", "boolean", FieldType.BOOLEAN, List.of(), List.of("lucene"), List.of(), false)
        );

        ComparisonOperands result = ConversionUtils.extractComparisonOperands(equalsCall, fieldStorage);

        assertEquals("active", result.fieldName());
        assertNotNull("Value should not be null", result.value());
        assertTrue(
            "Value should be a Boolean instance, got: " + result.value().getClass().getSimpleName(),
            result.value() instanceof Boolean
        );
        assertEquals(Boolean.TRUE, result.value());
    }

    /**
     * Tests that extractComparisonOperands correctly extracts Boolean false as Boolean.FALSE.
     * Validates Requirement 4.6 (BOOLEAN → Boolean).
     */
    public void testExtractComparisonOperandsBooleanFalseProducesBoolean() {
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RexNode inputRef = rexBuilder.makeInputRef(intType, 0);
        RexNode boolLiteral = rexBuilder.makeLiteral(false, boolType);

        RexCall equalsCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, inputRef, boolLiteral);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("enabled", "boolean", FieldType.BOOLEAN, List.of(), List.of("lucene"), List.of(), false)
        );

        ComparisonOperands result = ConversionUtils.extractComparisonOperands(equalsCall, fieldStorage);

        assertEquals("enabled", result.fieldName());
        assertNotNull("Value should not be null", result.value());
        assertTrue(
            "Value should be a Boolean instance, got: " + result.value().getClass().getSimpleName(),
            result.value() instanceof Boolean
        );
        assertEquals(Boolean.FALSE, result.value());
    }

    // ===== Unit tests for extractInOperands (Task 6.2) =====

    /**
     * A SqlFunction for constructing IN-like RexCalls in tests.
     * Uses SqlKind.OTHER_FUNCTION and OperandTypes.VARIADIC to bypass Calcite's
     * operand validation assertions. The extractInOperands method only inspects
     * operand types (RexInputRef vs RexLiteral), not the operator's SqlKind.
     */
    private static final SqlFunction IN_FUNCTION = new SqlFunction(
        "IN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /**
     * Tests that extractInOperands throws IllegalArgumentException when the first
     * operand is a RexLiteral instead of RexInputRef.
     * Validates Requirement 9.7.
     */
    public void testExtractInOperandsThrowsWhenFirstOperandIsLiteral() {
        RexNode literal1 = rexBuilder.makeLiteral("active");
        RexNode literal2 = rexBuilder.makeLiteral("pending");

        RexCall inCall = (RexCall) rexBuilder.makeCall(IN_FUNCTION, literal1, literal2);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("status", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ConversionUtils.extractInOperands(inCall, fieldStorage)
        );
        assertTrue(
            "Exception message should mention RexInputRef requirement",
            exception.getMessage().contains("First operand of IN must be RexInputRef")
        );
    }

    /**
     * Tests that extractInOperands throws IllegalArgumentException when a value operand
     * (index > 0) is a RexInputRef instead of RexLiteral.
     * Validates Requirement 9.8.
     */
    public void testExtractInOperandsThrowsWhenValueOperandIsInputRef() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RexNode fieldRef = rexBuilder.makeInputRef(intType, 0);
        RexNode valueLiteral = rexBuilder.makeLiteral(42, intType, true);
        RexNode anotherRef = rexBuilder.makeInputRef(intType, 1);

        RexCall inCall = (RexCall) rexBuilder.makeCall(IN_FUNCTION, fieldRef, valueLiteral, anotherRef);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("status", "integer", FieldType.INTEGER, List.of(), List.of("lucene"), List.of(), false),
            new FieldStorageInfo("count", "integer", FieldType.INTEGER, List.of(), List.of("lucene"), List.of(), false)
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ConversionUtils.extractInOperands(inCall, fieldStorage)
        );
        assertTrue(
            "Exception message should mention operand index",
            exception.getMessage().contains("Operand at index 2 of IN must be RexLiteral")
        );
    }

    /**
     * Tests that extractInOperands throws IllegalArgumentException when the RexCall
     * has fewer than 2 operands (empty IN list).
     * Validates Requirement 9.9.
     */
    public void testExtractInOperandsThrowsForFewerThanTwoOperands() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RexNode fieldRef = rexBuilder.makeInputRef(intType, 0);

        RexCall inCall = (RexCall) rexBuilder.makeCall(IN_FUNCTION, fieldRef);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("status", "integer", FieldType.INTEGER, List.of(), List.of("lucene"), List.of(), false)
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ConversionUtils.extractInOperands(inCall, fieldStorage)
        );
        assertTrue("Exception message should mention minimum operand count", exception.getMessage().contains("at least 2 operands"));
    }

    /**
     * Tests correct extraction of multiple values with mixed types (String, Integer, Long).
     * Validates Requirements 9.1, 9.2, 9.3.
     */
    public void testExtractInOperandsMultipleValuesWithMixedTypes() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);

        RexNode fieldRef = rexBuilder.makeInputRef(intType, 0);
        RexNode stringValue = rexBuilder.makeLiteral("active");
        RexNode intValue = rexBuilder.makeLiteral(42, intType, true);
        RexNode longValue = rexBuilder.makeLiteral(9999999999L, bigintType, true);

        RexCall inCall = (RexCall) rexBuilder.makeCall(IN_FUNCTION, fieldRef, stringValue, intValue, longValue);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("status", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        InOperands result = ConversionUtils.extractInOperands(inCall, fieldStorage);

        assertEquals("status", result.fieldName());
        assertEquals("Should have 3 values", 3, result.values().size());
        assertTrue("First value should be String", result.values().get(0) instanceof String);
        assertEquals("active", result.values().get(0));
        assertTrue("Second value should be Integer", result.values().get(1) instanceof Integer);
        assertEquals(42, result.values().get(1));
        assertTrue("Third value should be Long", result.values().get(2) instanceof Long);
        assertEquals(9999999999L, result.values().get(2));
    }

    /**
     * Tests that a single-value IN produces a single-element list.
     * Validates Requirement 9.1.
     */
    public void testExtractInOperandsSingleValueProducesSingleElementList() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RexNode fieldRef = rexBuilder.makeInputRef(varcharType, 0);
        RexNode value = rexBuilder.makeLiteral("active");

        RexCall inCall = (RexCall) rexBuilder.makeCall(IN_FUNCTION, fieldRef, value);

        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("status", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
        );

        InOperands result = ConversionUtils.extractInOperands(inCall, fieldStorage);

        assertEquals("status", result.fieldName());
        assertEquals("Should have exactly 1 value", 1, result.values().size());
        assertEquals("active", result.values().get(0));
    }
}
