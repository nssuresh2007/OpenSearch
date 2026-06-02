/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * A top-level aggregation classified as eligible for backend delegation,
 * paired with the chosen backend's identifier.
 *
 * @param agg             the aggregation builder (carries type, field references, parameters, sub-aggs)
 * @param chosenBackendId the backend selected by the deterministic selection rule
 * @opensearch.internal
 */
public record EligibleAggregation(AggregationBuilder agg, String chosenBackendId) {
}
