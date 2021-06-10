/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.String.format;

public class SpeculativeQueryTaskTracker
{
    private static final Logger log = Logger.get(SpeculativeQueryTaskTracker.class);

    private final List<RemoteTask> allTasks = new ArrayList();
    private final Map<TaskId, Integer> taskIdIndexMap = new HashMap<>();
    private final Set<TaskId> completedTaskIds = newConcurrentHashSet();
    private boolean taskSchedulingCompleted;

    public void trackTask(RemoteTask task)
    {
        allTasks.add(task);
        taskIdIndexMap.put(task.getTaskId(), allTasks.size() - 1);
    }

    public void completeTaskScheduling()
    {
        this.taskSchedulingCompleted = true;
    }

    public void recordTaskFinish(TaskInfo taskInfo)
    {
        completedTaskIds.add(taskInfo.getTaskId());
        if (taskSchedulingCompleted) {
            checkAndCancelDuplicateTasks(taskInfo);
        }
    }

    private void checkAndCancelDuplicateTasks(TaskInfo taskInfo)
    {
        int sz = allTasks.size();
        int idx = taskIdIndexMap.get(taskInfo.getTaskId());
        int rightNeighborIdx = (idx + 1) % sz;
        int rightSkipNeighborIdx = (idx + 2) % sz;
        int leftNeighborIdx = (idx - 1 + sz) % sz;
        int leftSkipNeighborIdx = (idx - 2 + sz) % sz;

        if (completedTaskIds.contains(allTasks.get(rightSkipNeighborIdx).getTaskId()) && !completedTaskIds.contains(allTasks.get(rightNeighborIdx).getTaskId())) {
            // right skip neighbor also finished. So you can cancel rightNeighbor task
            allTasks.get(rightNeighborIdx).cancel();
            log.info(format("NIKHIL cancelled rightNeighbor_task = %d, for completed task= %d", rightNeighborIdx, idx));
        }

        if (completedTaskIds.contains(allTasks.get(leftSkipNeighborIdx).getTaskId()) && !completedTaskIds.contains(allTasks.get(leftNeighborIdx).getTaskId())) {
            // left skip neighbor also finished. So you can cancel leftNeighbor task
            allTasks.get(leftNeighborIdx).cancel();
            log.info(format("NIKHIL cancelled leftNeighbor_task= %d, for completed task= %d", leftNeighborIdx, idx));
        }
    }
}
