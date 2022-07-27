/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.search.SearchBackpressureSettings;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Search Tasks Elapsed Timer
 */
public class SearchTaskElapsedTimeTracker implements SearchTasksResourceTracker {
    @Override
    public void startIteration() {

    }

    @Override
    public boolean checkLimitBreached() {
        return false;
    }

    @Override
    public boolean isLimitConsecutivelyBreached() {
        return false;
    }

    @Override
    public Collection<Task> getEligibleTasksForCancellation(Collection<Task> taskList) {
        return taskList.stream()
            .filter(t -> System.currentTimeMillis() - t.getStartTime()
                > SearchBackpressureSettings.ELAPSED_TIME_THRESHOLD_SEARCH_TASK_MILLIS)
            .collect(Collectors.toList());
    }
}
