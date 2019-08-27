/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.impl.connector.source.coordinator;

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.SplitsAssignment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A class that helps track the uncheckpointed splits assignments per checkpoint.
 */
class UncheckpointedSplitsAssignment<SplitT extends SourceSplit> {
	private final SortedMap<Long, Map<Integer, List<SplitT>>> assignmentsByCheckpoints;
	private Map<Integer, List<SplitT>> uncheckpointedAssignments;

	UncheckpointedSplitsAssignment() {
		assignmentsByCheckpoints = new TreeMap<>();
		uncheckpointedAssignments = new HashMap<>();
	}

	void recordNewAssignment(SplitsAssignment<SplitT> assignment) {
		if (assignment.type() == SplitsAssignment.Type.INCREMENTAL) {
			assignment.assignment().forEach((id, splits) -> {
				uncheckpointedAssignments.computeIfAbsent(id, ignored -> new ArrayList<>())
										 .addAll(splits);
			});
		} else {
			throw new UnsupportedOperationException("The OVERRIDING assignment is not supported yet.");
		}
	}

	void snapshotState(long checkpointId) {
		assignmentsByCheckpoints.put(checkpointId, uncheckpointedAssignments);
		uncheckpointedAssignments = new HashMap<>();
	}

	void onCheckpointCompleted(long checkpointId) {
		assignmentsByCheckpoints.entrySet().removeIf(entry -> entry.getKey() <= checkpointId);
	}

	List<SplitT> splitsToAddBack(int subtaskId) {
		List<SplitT> splits = new ArrayList<>();
		assignmentsByCheckpoints.values().forEach(assignments -> {
			List<SplitT> splitsForSubtask = assignments.remove(subtaskId);
			if (splitsForSubtask != null) {
				splits.addAll(splitsForSubtask);
			}
		});
		return splits;
	}
}
