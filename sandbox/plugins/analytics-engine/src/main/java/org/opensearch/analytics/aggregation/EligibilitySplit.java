/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.opensearch.search.aggregations.AggregatorFactories;

import java.util.List;
import java.util.Map;

/**
 * Result of the eligibility classification: two disjoint subsets of the original
 * aggregation tree's top-level aggregations, plus a reasons map for diagnostics.
 *
 * @param eligible   aggregations delegated to a chosen backend (each carries its backend id)
 * @param ineligible aggregations that will run through the standard Lucene aggregator path
 * @param reasons    per-aggregation-name reason for ineligibility (for logging / explain)
 * @opensearch.internal
 */
public record EligibilitySplit(List<EligibleAggregation> eligible, AggregatorFactories.Builder ineligible, Map<
    String,
    IneligibilityReason> reasons) {
}
