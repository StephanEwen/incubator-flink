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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connectors.source.ReaderInfo;
import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.SplitsAssignment;
import org.apache.flink.api.connectors.source.event.AddSplitEvent;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.metrics.MetricGroup;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

public class SourceCoordinatorContextImpl<SplitT extends SourceSplit> implements SourceCoordinatorContext<SplitT> {
	private ExecutorNotifier notifier;
	private final Map<Integer, ReaderInfo> registeredReaders;
	private final SplitAssignmentTracker<SplitT> assignmentTracker;

	public SourceCoordinatorContextImpl(ExecutorNotifier notifier) {
		this.notifier = notifier;
		this.registeredReaders = new HashMap<>();
		this.assignmentTracker = new SplitAssignmentTracker<>();
	}

	@Override
	public MetricGroup metricGroup() {
		return null;
	}

	@Override
	public CompletableFuture<Boolean> sendEventToSourceReader(int subtaskId, SourceEvent event) {
		return null;
	}

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		return null;
	}

	@Override
	public int numSubtasks() {
		return 0;
	}

	@Override
	public Map<Integer, ReaderInfo> registeredReaders() {
		return Collections.unmodifiableMap(registeredReaders);
	}

	@Override
	public Map<Integer, List<SplitT>> currentAssignment() {
		return assignmentTracker.currentSplitsAssignment();
	}

	@Override
	public void assignSplits(SplitsAssignment<SplitT> assignment) {
		if (assignment.type() == SplitsAssignment.Type.OVERRIDING) {
			throw new UnsupportedOperationException("The OVERRIDING assignment type is not " +
													"supported yet.");
		}
		assignmentTracker.recordSplitAssignment(assignment);
		assignment.assignment().forEach(
				(id, splits) -> sendEventToSourceOperator(id, new AddSplitEvent<>(splits))
		);
	}

	@Override
	public void notifyNewAssignment() {
		notifier.notifyReady();
	}

	@Override
	public <T> void notifyNewAssignmentAsync(Callable<T> callable,
									  BiFunction<T, Throwable, Boolean> handler,
									  long initialDelay,
									  long period) {
		notifier.notifyReadyAsync(callable, handler, initialDelay, period);
	}

	@Override
	public <T> void notifyNewAssignmentAsync(Callable<T> callable, BiFunction<T, Throwable, Boolean> handler) {
		notifier.notifyReadyAsync(callable, handler);
	}

	@Override
	public CompletableFuture<Boolean> sendEventToSourceOperator(int subtaskId, OperatorEvent event) {
		return null;
	}

	// ------------ package private for source coordinator to use only -----------------
	void snapshotState(long checkpointId) {
		assignmentTracker.snapshotState(checkpointId);
	}

	UncheckpointedSplitsAssignment<SplitT> uncheckpointedSplitsAssignment() {
		return assignmentTracker.uncheckpointedSplitsAssignment();
	}

	void registerSourceReader(int subtaskId, ReaderInfo readerInfo) {
		registeredReaders.put(subtaskId, readerInfo);
	}

	List<SplitT> getAndRemoveUncheckpointedAssignment(int failedSubtaskId) {
		return assignmentTracker.getAndRemoveUncheckpointedAssignment(failedSubtaskId);
	}
}
