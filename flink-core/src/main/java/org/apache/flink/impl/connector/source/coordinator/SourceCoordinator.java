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
import org.apache.flink.api.connectors.source.SplitEnumerator;
import org.apache.flink.api.connectors.source.SplitsAssignment;
import org.apache.flink.api.connectors.source.event.AddSplitEvent;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.ReaderFailedEvent;
import org.apache.flink.api.connectors.source.event.ReaderRegistrationEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A class that runs a {@link org.apache.flink.api.connectors.source.SplitEnumerator}.
 * It is responsible for the followings:
 * 1. Maintain the assigned but uncheckpointed splits assignments.
 * 2. Handle the source reader registration.
 * 3. Handle the source reader failure.
 *
 * <p>This class has an internal thread that follows an event-loop model. In most cases, the
 * {@link SplitEnumerator} does not need to have its own internal thread because any split
 * assignment change is going to be triggered by some events and some corresponding method
 * of the SplitEnumerator will be invoked. If a new assignment is needed, the SplitEnumerator
 * can just complete the future that was returned earlier. Every time after a new assignment
 * is triggered, the main thread will get the future for the next assignment.
 *
 * <p>The first split assignment query is always triggered by a SourceReader registration.
 * After that the split assignment query can be triggered by one of the following:
 * 1. receiving a new SourceEvent from the SourceReader
 * 2. some splits are added back to the enumerator, probably due to a source reader failure.
 * 3. a new split is discovered (by an internal thread)
 */
public class SourceCoordinator<SplitT extends SourceSplit, CheckpointT> implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinator.class);
	private final SplitEnumerator<SplitT, CheckpointT> enumerator;
	private final Map<Integer, ReaderInfo> registeredReaders;
	private final SplitAssignmentTracker<SplitT> assignmentTracker;
	private final SimpleVersionedSerializer<CheckpointT> enumeratorStateSerializer;
	private final SimpleVersionedSerializer<SplitT> splitSerializer;
	private final ValueState<byte[]> coordinatorState;
	private final SourceCoordinatorContext context;

	public SourceCoordinator(SplitEnumerator<SplitT, CheckpointT> enumerator,
							 SimpleVersionedSerializer<CheckpointT> enumeratorStateSerializer,
							 SimpleVersionedSerializer<SplitT> splitsSerializer,
							 SourceCoordinatorContext context) {
		this.enumerator = enumerator;
		this.registeredReaders = new HashMap<>();
		this.assignmentTracker = new SplitAssignmentTracker<>();
		this.enumeratorStateSerializer = enumeratorStateSerializer;
		this.splitSerializer = splitsSerializer;
		this.coordinatorState = context.getState(new ValueStateDescriptor<>("CoordinatorState", byte[].class));
		this.context = context;
		this.enumerator.setSplitEnumeratorContext(context);
	}

	@Override
	public void close() throws Exception {
		enumerator.close();
	}

	/**
	 * Snapshot the state of assigner tracker and split enumerator.
	 *
	 * @param checkpointId the checkpoint id.
	 */
	public void snapshotState(long checkpointId) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		try {
			assignmentTracker.snapshotState(checkpointId);

			CoordinatorState<SplitT, CheckpointT> coordState = new CoordinatorState<>(
					checkpointId,
					enumerator,
					assignmentTracker.uncheckpointedSplitsAssignment(),
					splitSerializer,
					enumeratorStateSerializer);
			coordinatorState.update(coordState.toBytes());
			future.complete(true);
		} catch (IOException e) {
			LOG.warn("Failed to take snapshot on the SourceCoordinator.");
			future.complete(false);
		}
	}

	/**
	 * Handles the operator event sent from the source operator of the given subtask id.
	 *
	 * @param subtaskId the subtask id of the operator event sender.
	 * @param event the received operator event.
	 */
	void handleOperatorEvent(int subtaskId, OperatorEvent event) {
		if (event instanceof SourceEvent) {
			enumerator.handleSourceEvent(subtaskId, (SourceEvent) event);
		} else if (event instanceof ReaderRegistrationEvent) {
			handleReaderRegistrationEvent((ReaderRegistrationEvent) event);
		} else if (event instanceof ReaderFailedEvent) {
			handleReaderFailedEvent((ReaderFailedEvent) event);
		}
	}

	/**
	 * Update the split assignment. Record the
	 */
	void updateAssignment() {
		enumerator.nextAssignment(Collections.unmodifiableMap(registeredReaders),
								  assignmentTracker.currentSplitsAssignment(),
								  context.numSubtasks())
				  .ifPresent(assignment -> {
					  if (assignment.type() == SplitsAssignment.Type.OVERRIDING) {
						  throw new UnsupportedOperationException("The OVERRIDING assignment type is not " +
																  "supported yet.");
					  }
					  assignmentTracker.recordSplitAssignment(assignment);
					  assignment.assignment().forEach(
					  		(id, splits) -> context.sendEventToSourceOperator(id, new AddSplitEvent<>(splits))
					  );
				  });
	}

	// --------------------- private methods
	private void handleReaderRegistrationEvent(ReaderRegistrationEvent event) {
		registeredReaders.put(event.subtaskId(), new ReaderInfo(event.subtaskId(), event.location()));

	}

	private void handleReaderFailedEvent(ReaderFailedEvent event) {
		List<SplitT> splitsToAddBack = assignmentTracker.getAndRemoveUncheckpointedAssignment(event.subtaskId());
		enumerator.addSplitsBack(splitsToAddBack);
	}
}
