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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class that is responsible for tracking the past split assignments made by
 * {@link org.apache.flink.api.connectors.source.SplitEnumerator}.
 */
public class SplitAssignmentTracker<SplitT extends SourceSplit> {
	private final UncheckpointedSplitsAssignment<SplitT> uncheckpointedAssignment;
	private final Map<Integer, List<SplitT>> currentAssignment;

	public SplitAssignmentTracker() {
		uncheckpointedAssignment = new UncheckpointedSplitsAssignment<>();
		currentAssignment = new HashMap<>();
	}

	/**
	 * Take a snapshot of the uncheckpointed split assignments.
	 *
	 * @param checkpointId the id of the ongoing checkpoint
	 * @return the uncheckpointed splits assignment that needs to be saved.
	 */
	public Map<Long, Map<Integer, List<SplitT>>> snapshotState(long checkpointId) {
		uncheckpointedAssignment.snapshotState(checkpointId);
		return uncheckpointedAssignment.assignmentsByCheckpoints();
	}

	/**
	 * Get the current split assignment.
	 *
	 * @return the current split assignment.
	 */
	Map<Integer, List<SplitT>> currentSplitsAssignment() {
		return Collections.unmodifiableMap(currentAssignment);
	}

	/**
	 * Record a new split assignment.
	 *
	 * @param splitsAssignment the new split assignment.
	 */
	public void recordSplitAssignment(SplitsAssignment<SplitT> splitsAssignment) {
		splitsAssignment.assignment().forEach((id, splits) -> {
			currentAssignment.computeIfAbsent(id, ignored -> new ArrayList<>())
							 .addAll(splits);
		});
		uncheckpointedAssignment.recordNewAssignment(splitsAssignment);
	}

	/**
	 * Get the split to put back. This only happens when a source reader subtask has failed.
	 *
	 * @param failedSubtaskId the failed subtask id.
	 * @return A list of splits that needs to be added back to the
	 *         {@link org.apache.flink.api.connectors.source.SplitEnumerator}.
	 */
	public List<SplitT> getAndRemoveUncheckpointedAssignment(int failedSubtaskId) {
		List<SplitT> toPutBack = uncheckpointedAssignment.splitsToAddBack(failedSubtaskId);
		currentAssignment.get(failedSubtaskId).removeAll(toPutBack);
		return toPutBack;
	}
}
