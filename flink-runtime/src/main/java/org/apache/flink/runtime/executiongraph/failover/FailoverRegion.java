/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * FailoverRegion manages the failover of a minimal pipeline connected sub graph.
 * It will change from CREATED to CANCELING and then to CANCELLED and at last to RUNNING,
 */
public class FailoverRegion {

	private static final AtomicReferenceFieldUpdater<FailoverRegion, JobStatus> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(FailoverRegion.class, JobStatus.class, "state");

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(FailoverRegion.class);

	// ------------------------------------------------------------------------

	// a unique id for debugging
	private final ResourceID id = ResourceID.generate();

	/** Current status of the job execution */
	private volatile JobStatus state = JobStatus.RUNNING;

	private final ExecutionGraph executionGraph;

	private final List<ExecutionVertex> connectedExecutionVertexes;

	public FailoverRegion(ExecutionGraph executionGraph, List<ExecutionVertex> connectedExecutions) {
		this.executionGraph = checkNotNull(executionGraph);
		this.connectedExecutionVertexes = checkNotNull(connectedExecutions);
		LOG.info("FailoverRegion {} contains {}.", id.toString(), connectedExecutions.toString());
	}

	public void onExecutionFail(ExecutionVertex ev, Throwable cause) {
		//TODO: check if need precedings failover
		if (!executionGraph.getRestartStrategy().canRestart()) {
			executionGraph.failGlobal(new FlinkException("RestartStrategy "));
		}
		else {
			cancel();
		}
	}

	private void allVerticesInTerminalState() {
		while (true) {
			JobStatus curStatus = this.state;
			if (curStatus.equals(JobStatus.CANCELLING)) {
				if (transitionState(curStatus, JobStatus.CANCELED)) {
					reset();
					break;
				}
			}
			else {
				LOG.info("FailoverRegion {} is {} when allVerticesInTerminalState.", id.toString(), state);
				break;
			}
		}
	}

	public JobStatus getState() {
		return state;
	}

	/**
	 * get all execution vertexes contained in this region
	 */
	public List<ExecutionVertex> getAllExecutionVertexes() {
		return connectedExecutionVertexes;
	}

	// Notice the region to failover, 
	private void failover() {
		if (!executionGraph.getRestartStrategy().canRestart()) {
			executionGraph.failGlobal(new FlinkException("RestartStrategy validate fail"));
		}
		else {
			JobStatus curStatus = this.state;
			if (curStatus.equals(JobStatus.RUNNING)) {
				cancel();
			}
			else if (curStatus.equals(JobStatus.CANCELED)) {
				reset();
			}
			else {
				LOG.info("FailoverRegion {} is {} when notified to failover.", id.toString(), state);
			}
		}
	}

	// cancel all executions in this sub graph
	private void cancel() {
		while (true) {
			JobStatus curStatus = this.state;
			if (curStatus.equals(JobStatus.RUNNING)) {
				if (transitionState(curStatus, JobStatus.CANCELLING)) {

					// we build a future that is complete once all vertices have reached a terminal state
					final ArrayList<Future<?>> futures = new ArrayList<>(connectedExecutionVertexes.size());

					// cancel all tasks (that still need cancelling)
					for (ExecutionVertex vertex : connectedExecutionVertexes) {
						futures.add(vertex.cancel());
					}

					final FutureUtils.ConjunctFuture allTerminal = FutureUtils.combineAll(futures);
					allTerminal.thenAccept(new AcceptFunction<Void>() {
						@Override
						public void accept(Void value) {
							allVerticesInTerminalState();
						}
					});
					break;
				}
			}
			else {
				LOG.info("FailoverRegion {} is {} when cancel.", id.toString(), state);
				break;
			}
		}
	}

	// reset all executions in this sub graph
	private void reset() {
		try {
			//reset all connected ExecutionVertexes
			Collection<CoLocationGroup> colGroups = new HashSet<>();
			for (ExecutionVertex ev : connectedExecutionVertexes) {
				CoLocationGroup cgroup = ev.getJobVertex().getCoLocationGroup();
				if(cgroup != null && !colGroups.contains(cgroup)){
					cgroup.resetConstraints();
					colGroups.add(cgroup);
				}
				ev.resetForNewExecution();
			}
			if (transitionState(JobStatus.CANCELED, JobStatus.CREATED)) {
				restart();
			}
			else {
				LOG.info("FailoverRegion {} switched from CANCELLING to CREATED fail, will fail this region again.", id.toString());
				failover();
			}
		} catch (Throwable e) {
			LOG.info("FailoverRegion {} reset fail, will failover again.", id.toString());
			failover();
		}
	}

	// restart all executions in this sub graph
	private void restart() {
		try {
			if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
				// if we have checkpointed state, reload it into the executions
				//TODO: checkpoint support restore part ExecutionVertex cp
				/**
				if (executionGraph.getCheckpointCoordinator() != null) {
					executionGraph.getCheckpointCoordinator().restoreLatestCheckpointedState(
							connectedExecutionVertexes, false, false);
				}
				*/
				//TODO, use restart strategy to schedule them.
				//restart all connected ExecutionVertexes
				for (ExecutionVertex ev : connectedExecutionVertexes) {
					try {
						//TODO: change with schedule mode
						ev.scheduleForExecution(executionGraph.getSlotProvider(), true);
								//executionGraph.isQueuedSchedulingAllowed());
					}
					catch (Throwable e) {
						failover();
					}
				}
			}
			else {
				LOG.info("FailoverRegion {} switched from CREATED to RUNNING fail, will fail this region again.", id.toString());
				failover();
			}
		} catch (Exception e) {
			LOG.info("FailoverRegion {} restart failed, failover again.", id.toString(), e);
			failover();
		}
	}

	private boolean transitionState(JobStatus current, JobStatus newState) {
		if (STATE_UPDATER.compareAndSet(this, current, newState)) {
			LOG.info("FailoverRegion {} switched from state {} to {}.", id.toString(), current, newState);
			return true;
		}
		else {
			return false;
		}
	}

}
