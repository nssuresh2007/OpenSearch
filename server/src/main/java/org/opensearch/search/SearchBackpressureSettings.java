/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.monitor.jvm.JvmStats;

/**
 * Search backpressure settings
 */
public class SearchBackpressureSettings {
    public static final boolean BACKPRESSURE_ENABLED = true;
    public static final double SEARCH_HEAP_MEM_SIZE = JvmStats.jvmStats().getMem().getHeapMax().getBytes() * 0.05;
    public static final float VARIANCE_THRESHOLD = 2.0f;
    public static final int JVM_HEAP_THRESHOLD = 70;
    public static final float CPU_UTILIZATION_THRESHOLD = 0.9f;
    public static final float MAX_TASKS_ALLOWED_TO_CANCEL = 0.01f; //10% of tasks
    public static final float CPU_TIME_THRESHOLD_SEARCH_TASK_NANOS = 15 * 1000 * 1000;
    public static final float ELAPSED_TIME_THRESHOLD_SEARCH_TASK_MILLIS = 30 * 1000;
    public static final double HEAP_THRESHOLD_SEARCH_TASK = JvmStats.jvmStats().getMem().getHeapMax().getBytes() * 0.005;
}
