/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.search.SearchBackpressureSettings;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Search backpressure Resource tracker
 */
public class SearchBackpressureResourceTracker {
    public static final int NUM_DATAPOINTS_TO_MONITOR = 3;

    enum TrackerType {
        //enum ordinal also defines the order of priority
        ELAPSED_TIME, CPU_CYCLE, HEAP_MEM
    }

    private static final Logger logger = LogManager.getLogger(SearchBackpressureResourceTracker.class);

    private TaskResourceTrackingService taskResourceTrackingService;
    private SearchTasksHeapMemoryTracker heapMemoryTracker;
    private SearchTasksCPUUtilizationTracker cpuUtilizationTracker;
    private SearchTaskElapsedTimeTracker elapsedTimeTracker;

    public SearchBackpressureResourceTracker(TaskResourceTrackingService taskResourceTrackingService) {
        this.taskResourceTrackingService = taskResourceTrackingService;
        heapMemoryTracker = new SearchTasksHeapMemoryTracker(taskResourceTrackingService);
        cpuUtilizationTracker = new SearchTasksCPUUtilizationTracker();
        elapsedTimeTracker = new SearchTaskElapsedTimeTracker();
    }

    public void startIteration() {
        logger.info("Starting next iteration");
        heapMemoryTracker.startIteration();
        cpuUtilizationTracker.startIteration();
    }

    public Collection<Task> getSearchShardTasks() {
        Collection<Task> taskList = new ArrayList<>(taskResourceTrackingService.getResourceAwareTasks().values());
        taskResourceTrackingService.refreshResourceStats(taskList.toArray(Task[]::new));
        return taskList;
    }

    public boolean isNodeInDuress() {
        return heapMemoryTracker.isLimitConsecutivelyBreached() || cpuUtilizationTracker.isLimitConsecutivelyBreached();
    }

    public List<Task> getEligibleTasksToCancel(Collection<Task> taskList) {
        //TODO: Define priority when we merge the lists
        TreeSet<Tuple<TrackerType, Task>> tasksToCancel = new TreeSet<>((t1, t2) -> t2.v1().compareTo(t1.v1()));
        tasksToCancel.addAll(elapsedTimeTracker.getEligibleTasksForCancellation(taskList).stream()
            .map(t -> new Tuple<>(TrackerType.ELAPSED_TIME, t)).collect(Collectors.toList()));
        tasksToCancel.addAll(cpuUtilizationTracker.getEligibleTasksForCancellation(taskList).stream()
            .map(t -> new Tuple<>(TrackerType.CPU_CYCLE, t)).collect(Collectors.toList()));
        tasksToCancel.addAll(heapMemoryTracker.getEligibleTasksForCancellation(taskList).stream()
            .map(t -> new Tuple<>(TrackerType.HEAP_MEM, t)).collect(Collectors.toList()));

        return tasksToCancel.stream().map(Tuple::v2).collect(Collectors.toList());
    }

}
