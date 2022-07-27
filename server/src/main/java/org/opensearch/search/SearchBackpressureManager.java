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
import org.opensearch.tasks.*;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.opensearch.search.SearchBackpressureSettings.SEARCH_HEAP_MEM_SIZE;

/**
 * Search Backpressure Manager
 */
public class SearchBackpressureManager {

    private static final Logger logger = LogManager.getLogger(SearchBackpressureManager.class);

    private final TaskResourceTrackingService taskResourceTrackingService;
    private final SearchBackpressureResourceTracker searchTasksResourceTracker;
    private final SearchBackpressureCancellationTracker cancellationTracker;

    public SearchBackpressureManager(TaskResourceTrackingService taskResourceTrackingService) {
        this.taskResourceTrackingService = taskResourceTrackingService;
        searchTasksResourceTracker = new SearchBackpressureResourceTracker(taskResourceTrackingService);
        cancellationTracker = new SearchBackpressureCancellationTracker();
    }

    public void cancelBreachedSearchTasks() {
        logger.info("Starting cancel breached search tasks");
        searchTasksResourceTracker.startIteration();
        if (cancellationTracker.taskCancelledWithinWindow()) {
            logger.info("Task cancelled within window. Monitoring and not taking any action");
            logger.info("Is node in duress still?: {}", searchTasksResourceTracker.isNodeInDuress());
            cancellationTracker.resetForNextIteration();
            return;
        }
        if (searchTasksResourceTracker.isNodeInDuress()) {
            Collection<Task> searchTasksList = searchTasksResourceTracker.getSearchShardTasks();
            double runningTaskConsumptionSum = searchTasksList.stream()
                .mapToDouble(t -> t.getTotalResourceUtilization(ResourceStats.MEMORY)).sum();
            logger.info("Current RunningTaskConsumptionSum: {}, Search heap size limit: {}",
                runningTaskConsumptionSum, SEARCH_HEAP_MEM_SIZE);
            if (runningTaskConsumptionSum > SEARCH_HEAP_MEM_SIZE) {
                List<Task> taskList = searchTasksResourceTracker.getEligibleTasksToCancel(searchTasksList);
                for (Task t : taskList) {
                    if (cancellationTracker.isSearchTasksCancellationLimitReached(taskList)) {
                        cancellationTracker.resetForNextIteration();
                        logger.info("Task cancellation limit reached, returning");
                        return;
                    }
                    if (t instanceof CancellableTask) {
                        CancellableTask cancellableTask = (CancellableTask) t;
                        if (!cancellableTask.isCancelled()) {
                            logger.info("Cancelling task {}, mem: {}", cancellableTask.getId(),
                                cancellableTask.getTotalResourceUtilization(ResourceStats.MEMORY));
                            cancellableTask.cancel("Resource consumption exceeded");
                            cancellationTracker.recordTaskCancellation(t);
                        }
                    }
                }
            }
        }
        cancellationTracker.resetForNextIteration();
    }

    public void signalTaskCompleted(long taskId) {
        cancellationTracker.signalTaskCompleted(taskId);
    }

}
