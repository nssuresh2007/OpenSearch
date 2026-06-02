/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

/**
 * Reason an aggregation was classified as ineligible for backend delegation.
 * Recorded in the {@link EligibilitySplit#reasons()} map for logging and explain output.
 *
 * @opensearch.internal
 */
public enum IneligibilityReason {
    /** At least one referenced field has no backend-supported doc-value format (e.g. no "parquet"). */
    NO_PARQUET_DOC_VALUES,
    /** The aggregation references a script (inline or stored). */
    USES_SCRIPT,
    /** The aggregation type has no mapping to any backend's supported aggregate functions. */
    UNSUPPORTED_AGGREGATION_TYPE,
    /** No installed backend with a non-null aggregationExecutor() can evaluate this aggregation. */
    NO_CAPABLE_BACKEND,
    /** At least one sub-aggregation is ineligible, making the parent ineligible. */
    SUB_AGGREGATION_INELIGIBLE,
    /** Sub-aggregations resolve to different chosen backends (not supported in v1). */
    SUB_AGGREGATION_DIFFERENT_BACKEND,
    /** A star-tree precompute path is available for this aggregation; prefer it. */
    STAR_TREE_AVAILABLE,
    /** The backend's executor cannot translate a specific parameter combination. */
    UNTRANSLATABLE_PARAMETER
}
