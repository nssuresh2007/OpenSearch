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
import org.apache.calcite.util.Sarg;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Registry of per-function query serializers for delegated predicates.
 * Each serializer converts a Calcite RexCall into serialized QueryBuilder bytes
 * that the Lucene backend can deserialize at the data node.
 */
final class QuerySerializerRegistry {

    private static final Map<ScalarFunction, DelegatedPredicateSerializer> SERIALIZERS = Map.ofEntries(
        Map.entry(ScalarFunction.MATCH, QuerySerializerRegistry::serializeMatch),
        Map.entry(ScalarFunction.MATCH_PHRASE, QuerySerializerRegistry::serializeMatchPhrase),
        Map.entry(ScalarFunction.MATCH_BOOL_PREFIX, QuerySerializerRegistry::serializeMatchBoolPrefix),
        Map.entry(ScalarFunction.MATCH_PHRASE_PREFIX, QuerySerializerRegistry::serializeMatchPhrasePrefix),
        Map.entry(ScalarFunction.MULTI_MATCH, QuerySerializerRegistry::serializeMultiMatch),
        Map.entry(ScalarFunction.QUERY_STRING, QuerySerializerRegistry::serializeQueryString),
        Map.entry(ScalarFunction.SIMPLE_QUERY_STRING, QuerySerializerRegistry::serializeSimpleQueryString),
        Map.entry(ScalarFunction.EQUALS, QuerySerializerRegistry::serializeEquals),
        Map.entry(ScalarFunction.IN, QuerySerializerRegistry::serializeIn),
        Map.entry(ScalarFunction.SARG_PREDICATE, QuerySerializerRegistry::serializeSarg)
    );

    private QuerySerializerRegistry() {}

    static Map<ScalarFunction, DelegatedPredicateSerializer> getSerializers() {
        return SERIALIZERS;
    }

    // TODO: Extract each serialize* method into its own dedicated class once we handle more parameters.
    // These methods are expected to grow significantly as optional parameters are added.

    private static byte[] serializeMatch(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (operator, analyzer, fuzziness, boost) and apply them to the QueryBuilder.
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(fieldName, fieldValue);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchPhrase(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (slop, analyzer, zero_terms_query) and apply them to the QueryBuilder.
        MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder(fieldName, fieldValue);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchBoolPrefix(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (analyzer, fuzziness, operator, minimum_should_match) and apply them to the QueryBuilder.
        MatchBoolPrefixQueryBuilder queryBuilder = new MatchBoolPrefixQueryBuilder(fieldName, fieldValue);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchPhrasePrefix(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (slop, analyzer, max_expansions, zero_terms_query) and apply them to the QueryBuilder.
        MatchPhrasePrefixQueryBuilder queryBuilder = new MatchPhrasePrefixQueryBuilder(fieldName, fieldValue);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMultiMatch(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<String> fields = ConversionUtils.extractFieldsFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: extract per-field boost values from operand 0 and pass to QueryBuilder
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (type, operator, analyzer, fuzziness, minimum_should_match) and apply them to the QueryBuilder.
        MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder(fieldValue, fields.toArray(String[]::new));
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeQueryString(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<String> fields = ConversionUtils.extractFieldsFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: extract per-field boost values from operand 0 and pass to QueryBuilder
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (default_operator, analyzer, allow_leading_wildcard) and apply them to the QueryBuilder.
        QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(fieldValue);
        for (String field : fields) {
            queryBuilder.field(field);
        }
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeSimpleQueryString(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<String> fields = ConversionUtils.extractFieldsFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: extract per-field boost values from operand 0 and pass to QueryBuilder
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (default_operator, analyzer, flags, minimum_should_match) and apply them to the QueryBuilder.
        SimpleQueryStringBuilder queryBuilder = new SimpleQueryStringBuilder(fieldValue);
        for (String field : fields) {
            queryBuilder.field(field);
        }
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    /**
     * Serializes an EQUALS predicate into TermQueryBuilder bytes.
     *
     * <p>Extracts field name and value from the RexCall operands, constructs a
     * TermQueryBuilder, and serializes via NamedWriteable protocol. The TermQueryBuilder
     * delegates type-specific query construction to MappedFieldType.termQuery() at the
     * data node.
     */
    private static byte[] serializeEquals(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.ComparisonOperands operands = ConversionUtils.extractComparisonOperands(call, fieldStorage);
        TermQueryBuilder queryBuilder = new TermQueryBuilder(operands.fieldName(), operands.value());
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    /**
     * Serializes an IN predicate into TermsQueryBuilder bytes.
     *
     * <p>Extracts field name and multiple values from the RexCall operands, constructs a
     * TermsQueryBuilder, and serializes via NamedWriteable protocol. The TermsQueryBuilder
     * delegates type-specific query construction to MappedFieldType.termsQuery() at the
     * data node, producing a Lucene TermsQuery that matches documents containing any of
     * the specified values in the inverted index.
     *
     * <p>Always uses TermsQueryBuilder even for single-value IN predicates
     * (no fallback to TermQueryBuilder) to keep the serializer logic simple.
     */
    private static byte[] serializeIn(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.InOperands operands = ConversionUtils.extractInOperands(call, fieldStorage);
        TermsQueryBuilder queryBuilder = new TermsQueryBuilder(operands.fieldName(), operands.values());
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    /**
     * Serializes a SARG_PREDICATE (Calcite's folded IN/BETWEEN/range-union) into
     * TermQueryBuilder or TermsQueryBuilder bytes.
     *
     * <p>Calcite folds {@code field IN (v1, v2, v3)} into {@code SEARCH(field, Sarg[v1, v2, v3])}.
     * This serializer extracts the point values from the Sarg and produces:
     * <ul>
     *   <li>A single-value Sarg → {@code TermQueryBuilder(field, value)}</li>
     *   <li>A multi-value Sarg → {@code TermsQueryBuilder(field, values)}</li>
     * </ul>
     *
     * <p>Only point-equality Sargs (where {@code sarg.isPoints()} is true) are supported.
     * Range Sargs (BETWEEN, GT/LT combinations) are not supported because the Lucene backend
     * uses only the inverted index (no BKD trees for range queries).
     *
     * @throws IllegalArgumentException if the Sarg contains ranges (non-point values)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static byte[] serializeSarg(RexCall call, List<FieldStorageInfo> fieldStorage) {
        // SEARCH(field, Sarg[...]) has exactly 2 operands: RexInputRef and RexLiteral(Sarg)
        RexNode fieldOperand = call.getOperands().get(0);
        RexNode sargOperand = call.getOperands().get(1);

        if (!(fieldOperand instanceof RexInputRef inputRef)) {
            throw new IllegalArgumentException(
                "First operand of SEARCH must be RexInputRef, got: " + fieldOperand.getClass().getSimpleName()
            );
        }
        if (!(sargOperand instanceof RexLiteral sargLiteral)) {
            throw new IllegalArgumentException(
                "Second operand of SEARCH must be RexLiteral(Sarg), got: " + sargOperand.getClass().getSimpleName()
            );
        }

        Sarg sarg = sargLiteral.getValueAs(Sarg.class);
        if (sarg == null) {
            throw new IllegalArgumentException("Sarg literal value is null");
        }
        if (sarg.isPoints() == false) {
            throw new IllegalArgumentException("Lucene backend only supports point-equality Sargs (IN-lists), got range Sarg: " + sarg);
        }

        String fieldName = FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
        SqlTypeName typeName = sargLiteral.getType().getSqlTypeName();

        // Extract point values from the Sarg's RangeSet via reflection.
        // Guava's RangeSet/Range are not on the compile classpath of this plugin,
        // but are available at runtime via Calcite's transitive dependency.
        List<Object> values = extractSargPoints(sarg, typeName);

        if (values.size() == 1) {
            TermQueryBuilder queryBuilder = new TermQueryBuilder(fieldName, values.get(0));
            return ConversionUtils.serializeQueryBuilder(queryBuilder);
        } else {
            TermsQueryBuilder queryBuilder = new TermsQueryBuilder(fieldName, values);
            return ConversionUtils.serializeQueryBuilder(queryBuilder);
        }
    }

    /**
     * Extracts point values from a Sarg using reflection to access Guava's RangeSet/Range
     * which are not on the compile classpath but available at runtime.
     */
    @SuppressWarnings("rawtypes")
    private static List<Object> extractSargPoints(Sarg sarg, SqlTypeName typeName) {
        try {
            // sarg.rangeSet is a com.google.common.collect.RangeSet<C>
            Object rangeSet = Sarg.class.getField("rangeSet").get(sarg);
            // RangeSet.asRanges() returns Set<Range<C>>
            Iterable<?> ranges = (Iterable<?>) rangeSet.getClass().getMethod("asRanges").invoke(rangeSet);

            List<Object> values = new ArrayList<>();
            for (Object range : ranges) {
                // Range.lowerEndpoint() returns the point value for singleton ranges
                Object endpoint = range.getClass().getMethod("lowerEndpoint").invoke(range);
                values.add(convertSargValue((Comparable<?>) endpoint, typeName));
            }
            return values;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to extract point values from Sarg: " + sarg, e);
        }
    }

    /**
     * Converts a Sarg endpoint value to the appropriate Java type for QueryBuilder construction.
     * Sarg stores values as their Calcite-internal representation (e.g., BigDecimal for numbers,
     * NlsString for strings). This method converts them to the types expected by TermQueryBuilder.
     */
    private static Object convertSargValue(Comparable<?> value, SqlTypeName typeName) {
        switch (typeName) {
            case VARCHAR:
            case CHAR:
                // Sarg stores strings as NlsString; extract the string value
                if (value instanceof org.apache.calcite.util.NlsString nls) {
                    return nls.getValue();
                }
                return value.toString();
            case INTEGER:
                if (value instanceof java.math.BigDecimal bd) {
                    return bd.intValueExact();
                }
                return ((Number) value).intValue();
            case BIGINT:
                if (value instanceof java.math.BigDecimal bd) {
                    return bd.longValueExact();
                }
                return ((Number) value).longValue();
            case FLOAT:
            case REAL:
                if (value instanceof java.math.BigDecimal bd) {
                    return bd.floatValue();
                }
                return ((Number) value).floatValue();
            case DOUBLE:
                if (value instanceof java.math.BigDecimal bd) {
                    return bd.doubleValue();
                }
                return ((Number) value).doubleValue();
            case BOOLEAN:
                return value;
            case TIMESTAMP:
            case DATE:
                if (value instanceof java.math.BigDecimal bd) {
                    return bd.longValueExact();
                }
                return ((Number) value).longValue();
            default:
                throw new IllegalArgumentException("Unsupported Sarg value type: " + typeName);
        }
    }
}
