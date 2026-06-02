/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.aggregation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Shard-scoped cache for per-aggregation eligibility classification results.
 * Keyed by {@code (aggregationType, fieldName, mapperEpoch)} so that the per-request
 * classification cost is reduced to a map lookup once a given combination has been seen.
 *
 * <p>Invalidated when the shard's {@code MapperService} epoch advances (mapping update).
 *
 * @opensearch.internal
 */
public class EligibilityCache {

    private final ConcurrentMap<EligibilityKey, AggregationEligibilityClassifier.ClassifyResult> cache = new ConcurrentHashMap<>();
    private volatile long currentEpoch;

    public EligibilityCache(long initialEpoch) {
        this.currentEpoch = initialEpoch;
    }

    /**
     * Look up a cached classification result.
     *
     * @return the cached result, or null if not present or epoch has advanced
     */
    public AggregationEligibilityClassifier.ClassifyResult get(String aggregationType, String fieldName, long mapperEpoch) {
        if (mapperEpoch != currentEpoch) {
            invalidate(mapperEpoch);
            return null;
        }
        return cache.get(new EligibilityKey(aggregationType, fieldName, mapperEpoch));
    }

    /**
     * Store a classification result in the cache.
     */
    public void put(String aggregationType, String fieldName, long mapperEpoch, AggregationEligibilityClassifier.ClassifyResult result) {
        if (mapperEpoch != currentEpoch) {
            invalidate(mapperEpoch);
        }
        cache.put(new EligibilityKey(aggregationType, fieldName, mapperEpoch), result);
    }

    /**
     * Invalidate the cache when the mapper epoch advances.
     */
    private void invalidate(long newEpoch) {
        if (newEpoch > currentEpoch) {
            currentEpoch = newEpoch;
            cache.clear();
        }
    }

    /**
     * Cache key combining aggregation type, field name, and mapper epoch.
     */
    record EligibilityKey(String aggregationType, String fieldName, long mapperEpoch) {
    }
}
