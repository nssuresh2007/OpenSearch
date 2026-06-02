/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Aggregation delegation framework for the analytics engine.
 *
 * <p>This package contains the components that intercept the standard aggregation phase,
 * classify aggregations for backend delegation eligibility, collect per-segment doc-ID
 * bitsets, and orchestrate execution on backend executors (e.g. DataFusion over Parquet).
 *
 * <p>Key classes:
 * <ul>
 *   <li>{@link org.opensearch.analytics.aggregation.DelegatingAggregationProcessor} — wraps the
 *       default aggregation processor and routes eligible aggregations to backends</li>
 *   <li>{@link org.opensearch.analytics.aggregation.AggregationEligibilityClassifier} — classifies
 *       each aggregation as eligible or ineligible for delegation</li>
 *   <li>{@link org.opensearch.analytics.aggregation.AggregationDelegationService} — orchestrates
 *       per-segment backend execution and combines partial results</li>
 *   <li>{@link org.opensearch.analytics.aggregation.DocIdBitsetCollectorManager} — collects matching
 *       doc IDs into per-segment FixedBitSets during the query phase</li>
 *   <li>{@link org.opensearch.analytics.aggregation.BackendRouter} — O(1) lookup of backend
 *       aggregation executors by backend ID</li>
 *   <li>{@link org.opensearch.analytics.aggregation.BitsetProviderRegistry} — Java-side registry
 *       for FFM callback access to per-segment bitsets</li>
 * </ul>
 */
package org.opensearch.analytics.aggregation;
