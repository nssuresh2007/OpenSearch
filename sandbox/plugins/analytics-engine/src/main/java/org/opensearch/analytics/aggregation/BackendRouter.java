/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendAggregationExecutor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-node registry providing O(1) lookup of {@link BackendAggregationExecutor} by backend id.
 *
 * <p>Supports two construction modes:
 * <ol>
 *   <li><b>Eager</b>: constructed with a pre-built executor map (for testing)</li>
 *   <li><b>Lazy</b>: constructed with a list of backend plugins; executors are resolved
 *       on first access to avoid triggering heavy static initializers at startup</li>
 * </ol>
 *
 * <p>Missing entries indicate the backend is not installed on this node; the delegation service
 * treats this as a reclassification to Ineligible at execution time.
 *
 * @opensearch.internal
 */
public class BackendRouter {

    private static final Logger logger = LogManager.getLogger(BackendRouter.class);

    private volatile Map<String, BackendAggregationExecutor> executors;
    private final List<AnalyticsSearchBackendPlugin> backendPlugins;

    /**
     * Creates a new backend router with the given executor map (eager mode).
     *
     * @param executors map from backend id to executor instance; must not be null
     */
    public BackendRouter(Map<String, BackendAggregationExecutor> executors) {
        this.executors = Collections.unmodifiableMap(executors);
        this.backendPlugins = null;
    }

    /**
     * Creates a new backend router that lazily resolves executors from the given
     * backend plugins on first access. This avoids triggering heavy static initializers
     * (e.g. Substrait extension loading) at plugin startup time.
     *
     * @param backendPlugins the discovered backend plugins to query for executors
     */
    public BackendRouter(List<AnalyticsSearchBackendPlugin> backendPlugins) {
        this.executors = null;
        this.backendPlugins = backendPlugins;
    }

    /**
     * Returns the executor for the given backend id, or {@code null} if no executor
     * is registered for that backend.
     *
     * @param backendId the backend identifier to look up
     * @return the executor, or {@code null} if not found
     */
    public BackendAggregationExecutor getExecutor(String backendId) {
        return getExecutors().get(backendId);
    }

    /**
     * Returns {@code true} if an executor is registered for the given backend id.
     */
    public boolean hasExecutor(String backendId) {
        return getExecutors().containsKey(backendId);
    }

    /**
     * Returns the number of registered executors.
     */
    public int size() {
        return getExecutors().size();
    }

    /**
     * Returns the executor map, lazily initializing it from backend plugins if needed.
     */
    private Map<String, BackendAggregationExecutor> getExecutors() {
        if (executors == null) {
            synchronized (this) {
                if (executors == null) {
                    Map<String, BackendAggregationExecutor> map = new HashMap<>();
                    if (backendPlugins != null) {
                        for (AnalyticsSearchBackendPlugin be : backendPlugins) {
                            try {
                                BackendAggregationExecutor executor = be.aggregationExecutor();
                                if (executor != null) {
                                    map.put(executor.backendId(), executor);
                                    logger.info("Registered aggregation executor [{}] from backend [{}]", executor.backendId(), be.name());
                                }
                            } catch (Exception e) {
                                logger.warn("Failed to obtain aggregation executor from backend [{}]: {}", be.name(), e.getMessage());
                            }
                        }
                    }
                    executors = Collections.unmodifiableMap(map);
                }
            }
        }
        return executors;
    }
}
