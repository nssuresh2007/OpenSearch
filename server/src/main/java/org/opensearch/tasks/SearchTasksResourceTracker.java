/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import java.util.Collection;

/**
 * Search Tasks Resource Tracker
 */
public interface SearchTasksResourceTracker {
    void startIteration();
    boolean checkLimitBreached();
    boolean isLimitConsecutivelyBreached();
    Collection<Task> getEligibleTasksForCancellation(Collection<Task> taskList);
}
