/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import com.sun.management.OperatingSystemMXBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.SearchBackpressureSettings;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;

import static org.opensearch.search.SearchBackpressureSettings.CPU_UTILIZATION_THRESHOLD;
import static org.opensearch.tasks.SearchBackpressureResourceTracker.NUM_DATAPOINTS_TO_MONITOR;

/**
 * Search Tasks CPU Utilization tracker
 */
public class SearchTasksCPUUtilizationTracker implements SearchTasksResourceTracker {

    private static final OperatingSystemMXBean osMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private static final Logger logger = LogManager.getLogger(SearchTasksCPUUtilizationTracker.class);
    private final LinkedList<Boolean> cpuThresholdBreached = new LinkedList<>();

    @Override
    public boolean checkLimitBreached() {
        return cpuThresholdBreached.getLast();
    }

    @Override
    public boolean isLimitConsecutivelyBreached() {
        if (cpuThresholdBreached.size() < NUM_DATAPOINTS_TO_MONITOR) return false;
        return cpuThresholdBreached.stream().allMatch(o -> o.equals(true));
    }

    @Override
    public void startIteration() {
        recordLimitBreached();
        logger.info("CPU History: {}", cpuThresholdBreached);
    }

    @Override
    public Collection<Task> getEligibleTasksForCancellation(Collection<Task> taskList) {
        return taskList.stream()
                .filter(t -> t.getTotalResourceUtilization(ResourceStats.CPU) > SearchBackpressureSettings.CPU_TIME_THRESHOLD_SEARCH_TASK_NANOS)
                .collect(Collectors.toList());
    }

    private void recordLimitBreached() {
        double processCpuLoad = osMXBean.getProcessCpuLoad();
        logger.info("Current CPU load: {}", processCpuLoad);
        boolean cpuBreached = processCpuLoad > CPU_UTILIZATION_THRESHOLD;
        // Record the breached history in the list
        cpuThresholdBreached.addLast(cpuBreached);
        if (cpuThresholdBreached.size() > NUM_DATAPOINTS_TO_MONITOR) cpuThresholdBreached.removeFirst();
    }
}
