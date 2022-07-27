/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.search.SearchBackpressureManager;

/**
 * Observes the task resource consumption periodically
 */
public class TaskResourceConsumptionObserver implements Runnable {

    private TaskResourceTrackingService taskResourceTrackingService;
    private SearchBackpressureManager searchBackpressureManager;

    public TaskResourceConsumptionObserver(TaskResourceTrackingService taskResourceTrackingService) {
        this.taskResourceTrackingService = taskResourceTrackingService;
        searchBackpressureManager = new SearchBackpressureManager(taskResourceTrackingService);
    }

    @Override
    public void run() {
        searchBackpressureManager.cancelBreachedSearchTasks();
    }

    public void signalTaskCompleted(long taskId) {
        searchBackpressureManager.signalTaskCompleted(taskId);
    }
}
