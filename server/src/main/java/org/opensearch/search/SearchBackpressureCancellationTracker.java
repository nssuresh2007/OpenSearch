/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.tasks.ResourceStats;
import org.opensearch.tasks.Task;

import java.util.*;

/**
 * Tracks the cancellation tasks for that iteration
 */
public class SearchBackpressureCancellationTracker {

    public static final int NUM_DATAPOINTS_TO_MONITOR = 3;
    private static final Logger logger = LogManager.getLogger(SearchBackpressureCancellationTracker.class);

    private int numSearchTasksCancelled;
    private Map<ResourceStats, Long> cancelledResourceStats;
    private final LinkedList<Boolean> cancellationHistory = new LinkedList<>();
    private boolean tasksCancelledInCurrentIteration = false;
    private final TreeSet<Long> completedTasks = new TreeSet<>();
    private final TreeSet<Long> cancelledTasks = new TreeSet<>();
    private long lastResetTime;

    public SearchBackpressureCancellationTracker() {
        cancelledResourceStats = new HashMap<>();
    }

    /**
     * Invoked after task's cancelled flag is set
     */
    public void recordTaskCancellation(Task task) {
        tasksCancelledInCurrentIteration = true;
        numSearchTasksCancelled++;
        cancelledResourceStats.put(ResourceStats.CPU,
            cancelledResourceStats.getOrDefault(ResourceStats.CPU, 0L) + task.getTotalResourceUtilization(ResourceStats.CPU));
        cancelledResourceStats.put(ResourceStats.MEMORY,
            cancelledResourceStats.getOrDefault(ResourceStats.MEMORY, 0L) + task.getTotalResourceUtilization(ResourceStats.MEMORY));
        cancelledTasks.add(task.getId());
    }

    public void resetForNextIteration() {
        numSearchTasksCancelled = 0;
        cancelledResourceStats.clear();
        cancellationHistory.addLast(tasksCancelledInCurrentIteration);
        if (cancellationHistory.size() > NUM_DATAPOINTS_TO_MONITOR) {
            cancellationHistory.removeFirst();
        }
        tasksCancelledInCurrentIteration = false;
        resetTaskCountersIfExpired();
    }

    public boolean taskCancelledWithinWindow() {
        return cancellationHistory.stream().anyMatch(c -> c==true);
    }

    public boolean isSearchTasksCancellationLimitReached(Collection<Task> searchTasksList) {
        int completedTaskCount = completedTasks.size();
        int cancelledTaskCount = cancelledTasks.size();
        logger.info("CompletedTaskCount: {} CancelledTaskCount: {}", completedTaskCount, cancelledTaskCount);
        return (double) cancelledTaskCount / completedTaskCount > SearchBackpressureSettings.MAX_TASKS_ALLOWED_TO_CANCEL;
    }

    private void resetTaskCountersIfExpired() {
        if (lastResetTime == 0 || System.currentTimeMillis() - lastResetTime >= 60000 ) {
            logger.info("Resetting task counter");
            lastResetTime = System.currentTimeMillis();
            completedTasks.clear();
            cancelledTasks.clear();
        }
    }

    public void signalTaskCompleted(long taskId) {
        completedTasks.add(taskId);
    }
}
