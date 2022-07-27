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
import org.opensearch.monitor.jvm.JvmStats;

import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.opensearch.search.SearchBackpressureSettings.*;
import static org.opensearch.tasks.SearchBackpressureResourceTracker.NUM_DATAPOINTS_TO_MONITOR;

/**
 * Search Tasks Heap Memory tracker
 */
public class SearchTasksHeapMemoryTracker implements SearchTasksResourceTracker {

    private static final Logger logger = LogManager.getLogger(SearchTasksHeapMemoryTracker.class);
    private final TaskResourceTrackingService taskResourceTrackingService;
    private final LinkedList<Boolean> heapThresholdBreached = new LinkedList<>();

    public SearchTasksHeapMemoryTracker(TaskResourceTrackingService taskResourceTrackingService) {
        this.taskResourceTrackingService = taskResourceTrackingService;
    }

    public double getCompletedTasksAverageHeapConsumption() {
        return taskResourceTrackingService.getCompletedTaskResourceConsumptionAvg();
    }

    public double getCurrentTasksAverageHeapConsumption(Collection<Task> taskList) {
        double runningTaskConsumptionSum = taskList.stream()
            .mapToDouble(t -> t.getTotalResourceUtilization(ResourceStats.MEMORY)).sum();
        return runningTaskConsumptionSum / taskList.size();
    }

    private void recordLimitBreached() {
        int heapPercent = JvmStats.jvmStats().getMem().getHeapUsedPercent();
        logger.info("Current heap percent: {}", heapPercent);
        boolean heapBreached = heapPercent > JVM_HEAP_THRESHOLD;
        heapThresholdBreached.addLast(heapBreached);
        if (heapThresholdBreached.size() > NUM_DATAPOINTS_TO_MONITOR) heapThresholdBreached.removeFirst();
    }

    @Override
    public void startIteration() {
        recordLimitBreached();
        logger.info("Heap Threshold history: {}", heapThresholdBreached);
    }

    @Override
    public boolean checkLimitBreached() {
        return heapThresholdBreached.getLast();
    }

    @Override
    public boolean isLimitConsecutivelyBreached() {
        if (heapThresholdBreached.size() < NUM_DATAPOINTS_TO_MONITOR) return false;
        return heapThresholdBreached.stream().allMatch(o -> o.equals(true));
    }

    @Override
    public Collection<Task> getEligibleTasksForCancellation(Collection<Task> taskList) {
        logger.info("In get eligible tasks for cancellation");
        double runningTaskConsumptionSum = taskList.stream()
            .mapToDouble(t -> t.getTotalResourceUtilization(ResourceStats.MEMORY)).sum();
        double runningTaskConsumptionAvg = runningTaskConsumptionSum / taskList.size();
        double completedTaskAvg = getCompletedTasksAverageHeapConsumption();
        logger.info("RunningTaskConsumptionSum:{} RunningTaskConsumptionAvg: {} CompletedTaskAvg: {}",
            runningTaskConsumptionSum, runningTaskConsumptionAvg, completedTaskAvg);

        TreeSet<Tuple<Long, Task>> resourceConsumptionSet
            = new TreeSet<>((o1, o2) -> Long.compare(o2.v1(), o1.v1()));
        resourceConsumptionSet.addAll(taskList.stream()
            .map(t -> new Tuple<>(t.getTotalResourceUtilization(ResourceStats.MEMORY), t))
            .collect(Collectors.toList()));
        logger.info("Resource consumption set: {}",
            resourceConsumptionSet.stream().map(t -> t.v1()+" consumption: " + t.v2().getTotalResourceUtilization(ResourceStats.MEMORY)));

        return resourceConsumptionSet.stream()
            .filter(tp -> hasTaskBreachedThreshold(tp.v1(), runningTaskConsumptionAvg, completedTaskAvg))
            .map(Tuple::v2)
            .collect(Collectors.toList());
    }

    private boolean hasTaskBreachedThreshold(double memConsumed, double runningTaskConsumptionAvg, double completedTaskConsumptionAvg) {
        double avgTaskConsumption = Math.max(runningTaskConsumptionAvg, completedTaskConsumptionAvg);
        float varianceThreshold = VARIANCE_THRESHOLD;
        logger.info("Running Task consumption: {} Completed Task consumption: {}, Current Task consumption {}, variance: {}",
            runningTaskConsumptionAvg, completedTaskConsumptionAvg, memConsumed, varianceThreshold);
        if (memConsumed < HEAP_THRESHOLD_SEARCH_TASK) {
            logger.info("memConsumed {} lesser than task heap threshold: {}", memConsumed, HEAP_THRESHOLD_SEARCH_TASK);
            return false;
        }
        if (avgTaskConsumption == 0) {
            return false;
        }
        //logger.info("Moving Avg: {}, Resource consumption: {}", movingAvg, resourceConsumption);
        //logger.info("Variance is {}, returning {}", resourceConsumption/movingAvg,
        //    (resourceConsumption/movingAvg > VARIANCE_THRESHOLD));
        boolean breached = memConsumed/avgTaskConsumption > varianceThreshold;
        if (breached) {
            logger.info("Moving Avg: {}, Resource consumption: {}", avgTaskConsumption, memConsumed);
        }
        return breached;
    }
}
