/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.golden;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.rel.RelNode;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.result.SearchResponseBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Golden file tests for the DSL query executor plugin.
 *
 * <p>Each test method loads its own specific golden file by name, keeping
 * dependencies explicit. Tests validate the forward path (DSL → RelNode),
 * reverse path (ExecutionResult → SearchResponse), and field name consistency.
 */
public class DslGoldenFileTests extends OpenSearchTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final NamedXContentRegistry X_CONTENT_REGISTRY = new NamedXContentRegistry(
        new SearchModule(org.opensearch.common.settings.Settings.EMPTY, Collections.emptyList()).getNamedXContents()
    );

    // ---- Forward Path Helper ----

    /**
     * Loads a golden file, parses the inputDsl into a SearchSourceBuilder,
     * invokes SearchSourceConverter.convert(), serializes the RelNode via
     * explain(), and compares against expectedRelNodePlan. In update mode,
     * overwrites the golden file instead of asserting.
     */
    private void runForwardPathTest(String goldenFileName) throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load(goldenFileName);
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
        assertFalse("No " + expectedType + " plan produced for " + goldenFileName, matchingPlans.isEmpty());

        RelNode relNode = matchingPlans.get(0).relNode();
        String actualPlan = relNode.explain().trim();

        if (GoldenFileUpdater.isUpdateMode()) {
            Path filePath = goldenFilePath(goldenFileName);
            GoldenFileUpdater.update(filePath, actualPlan, tc.getExpectedOutputDsl());
        } else {
            assertEquals(
                "Forward path mismatch for " + goldenFileName
                    + "\nExpected:\n" + tc.getExpectedRelNodePlan()
                    + "\nActual:\n" + actualPlan,
                tc.getExpectedRelNodePlan().trim(),
                actualPlan
            );
        }
    }

    // ---- Reverse Path Helper ----

    /**
     * Loads a golden file, constructs an ExecutionResult from executionRows
     * and field names, invokes SearchResponseBuilder.build(), serializes the
     * SearchResponse to JSON, and compares against expectedOutputDsl ignoring
     * non-deterministic fields (took, _shards). In update mode, overwrites
     * the golden file instead of asserting.
     */
    private void runReversePathTest(String goldenFileName) throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load(goldenFileName);
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

        // Build the RelNode so we can construct a proper QueryPlan for ExecutionResult
        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        QueryPlans.QueryPlan plan = plans.get(expectedType).get(0);

        // Convert golden file rows to Object[] iterable
        List<Object[]> rows = new ArrayList<>();
        for (List<Object> row : tc.getExecutionRows()) {
            rows.add(row.toArray());
        }
        ExecutionResult result = new ExecutionResult(plan, rows);

        // Build SearchResponse
        var response = SearchResponseBuilder.build(List.of(result), 0L);
        String responseJson = Strings.toString(MediaTypeRegistry.JSON, response);

        @SuppressWarnings("unchecked")
        Map<String, Object> actualOutput = MAPPER.readValue(responseJson, Map.class);

        if (GoldenFileUpdater.isUpdateMode()) {
            Path filePath = goldenFilePath(goldenFileName);
            GoldenFileUpdater.update(filePath, tc.getExpectedRelNodePlan(), actualOutput);
        } else {
            // Remove non-deterministic fields before comparison
            Map<String, Object> expectedOutput = tc.getExpectedOutputDsl();
            stripNonDeterministicFields(actualOutput);
            stripNonDeterministicFields(expectedOutput);

            assertEquals(
                "Reverse path mismatch for " + goldenFileName
                    + "\nExpected:\n" + MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(expectedOutput)
                    + "\nActual:\n" + MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(actualOutput),
                expectedOutput,
                actualOutput
            );
        }
    }

    // ---- Consistency Check Helper ----

    /**
     * Loads a golden file, runs the forward path to get the RelNode, and
     * verifies that the RelNode output field names match the executionFieldNames
     * in the golden file.
     */
    private void runConsistencyCheck(String goldenFileName) throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load(goldenFileName);
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        RelNode relNode = plans.get(expectedType).get(0).relNode();
        List<String> relNodeFields = relNode.getRowType().getFieldNames();

        assertEquals(
            "Field name consistency mismatch for " + goldenFileName
                + "\nRelNode fields: " + relNodeFields
                + "\nGolden file executionFieldNames: " + tc.getExecutionFieldNames(),
            tc.getExecutionFieldNames(),
            relNodeFields
        );
    }

    // ---- Utility Methods ----

    /**
     * Parses a golden file inputDsl map into a SearchSourceBuilder using
     * XContentParser with the full SearchModule registry (so query builders
     * like term, range, bool are recognized).
     */
    private SearchSourceBuilder parseSearchSource(Map<String, Object> inputDsl) throws IOException {
        String json = MAPPER.writeValueAsString(inputDsl);
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                X_CONTENT_REGISTRY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                json
            )
        ) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    /**
     * Resolves the file-system path to a golden file for update mode.
     * Falls back to classpath resource resolution.
     */
    private Path goldenFilePath(String goldenFileName) throws URISyntaxException {
        URL resource = getClass().getClassLoader().getResource("golden/" + goldenFileName);
        if (resource == null) {
            throw new IllegalStateException("Golden file not found on classpath: golden/" + goldenFileName);
        }
        return Path.of(resource.toURI());
    }

    /**
     * Removes non-deterministic fields (took, _shards) from a serialized
     * SearchResponse map so that comparisons are stable.
     */
    private void stripNonDeterministicFields(Map<String, Object> responseMap) {
        responseMap.remove("took");
        responseMap.remove("timed_out");
        responseMap.remove("_shards");
    }

    // ---- Common Runner ----

    /**
     * Runs forward path, reverse path, and consistency checks for a single
     * golden file.
     */
    private void runAllChecks(String goldenFileName) throws Exception {
        runForwardPathTest(goldenFileName);
        runReversePathTest(goldenFileName);
        runConsistencyCheck(goldenFileName);
    }

    // ---- Test Methods ----

    public void testMatchAllHits() throws Exception {
        runAllChecks("match_all_hits.json");
    }

    public void testTermsWithAvgAggregation() throws Exception {
        runAllChecks("terms_with_avg_aggregation.json");
    }
}
