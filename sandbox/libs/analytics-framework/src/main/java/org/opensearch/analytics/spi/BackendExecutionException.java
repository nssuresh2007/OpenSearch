/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Checked exception thrown by {@link BackendAggregationExecutor#executeOnSegment} when
 * the backend encounters a recoverable error during per-segment aggregation execution.
 *
 * <p>The delegation framework catches this exception and may fall back to the standard
 * Lucene aggregator path for the affected segment and aggregation. Non-recoverable errors
 * (e.g. corrupt Parquet file) should also be wrapped in this exception so the framework
 * can surface a descriptive {@code ShardSearchFailure} rather than propagating raw native
 * errors to the client.
 *
 * @opensearch.internal
 */
public class BackendExecutionException extends Exception {

    public BackendExecutionException(String message) {
        super(message);
    }

    public BackendExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
