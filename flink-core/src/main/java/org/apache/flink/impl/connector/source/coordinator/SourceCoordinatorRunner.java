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
import org.apache.flink.api.connectors.source.event.OperatorEvent;

import java.util.concurrent.ExecutorService;

/**
 * The container class for {@link SourceCoordinator}. It is host an executor to interact with the
 * {@link SourceCoordinator} which is thread-less.
 */
public class SourceCoordinatorRunner<T, SplitT extends SourceSplit, CheckpointT> implements AutoCloseable {
	private final ExecutorService executor;
	private final SourceCoordinator<SplitT, CheckpointT> coordinator;
	private final Runnable assignmentUpdateRunnable;

	public SourceCoordinatorRunner(ExecutorService executor,
								   SourceCoordinator<SplitT, CheckpointT> coordinator) {
		this.executor = executor;
		this.coordinator = coordinator;
		this.assignmentUpdateRunnable = coordinator::updateAssignment;
	}

	public void start() {
		executor.submit(new AssignmentPollingRunnable());
	}

	public void handleOperatorEvent(int subtaskId, OperatorEvent operatorEvent) {
		executor.execute(() -> coordinator.handleOperatorEvent(subtaskId, operatorEvent));
	}

	public void snapshotState(long checkpointId) {
		executor.execute(() -> coordinator.snapshotState(checkpointId));
	}

	@Override
	public void close() {
		executor.shutdown();
	}

	private class AssignmentPollingRunnable implements Runnable {
		@Override
		public void run() {
			coordinator.updateAssignment();
		}
	}
}
