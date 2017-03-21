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

import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple failover strategy that restarts each task individually.
 * This strategy is only applicable if the entire job consists unconnected
 * tasks, meaning each task is its own component.
 * 
 * <p>This strategy is mainly meant for testing and validation of the failure recovery model.
 */
public class RestartIndividualStrategy extends FailoverStrategy {

	static final Logger LOG = LoggerFactory.getLogger(RestartIndividualStrategy.class);

	// ------------------------------------------------------------------------

	/** The execution graph to recover */
	private final ExecutionGraph executionGraph;

	/**
	 * Creates a new failover strategy that recovers from failures by restarting all tasks
	 * of the execution graph.
	 * 
	 * @param executionGraph The execution graph to handle.
	 */
	public RestartIndividualStrategy(ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
	}

	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(final Execution taskExecution, Throwable cause) {

		LOG.info("Recovering task failure for {} (#{}) via individual restart.", 
				taskExecution.getVertex().getTaskNameWithSubtaskIndex(), taskExecution.getAttemptNumber());

		// trigger the restart once the task has reached its terminal state
		// Note: currently all tasks passed here are already in their terminal state,
		//       so we could actually avoid the future. We use it in this testing implementation
		//       to illustrate how to handle this thread / concurrency safe
		final Future<ExecutionState> terminationFuture = taskExecution.getTerminationFuture();

		terminationFuture.thenAcceptAsync(new AcceptFunction<ExecutionState>() {
			@Override
			public void accept(ExecutionState value) {
				try {
					Execution newExecution = taskExecution.getVertex().resetForNewExecution();
					newExecution.scheduleForExecution();
				}
				catch (Exception e) {
					executionGraph.failGlobal(
							new Exception("Error during fine grained recovery - triggering full recovery", e));
				}
			}
		}, executionGraph.getFutureExecutor());
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// we validate here that the vertices are in fact not connected to
		// any other vertices
		for (ExecutionJobVertex ejv : newJobVerticesTopological) {
			List<IntermediateResult> inputs = ejv.getInputs();
			IntermediateResult[] outputs = ejv.getProducedDataSets();

			if ((inputs != null && inputs.size() > 0) || (outputs != null && outputs.length > 0)) {
				throw new FlinkRuntimeException("Incompatible failover strategy - strategy '" + 
						getStrategyName() + "' can only handle jobs with only disconnected tasks.");
			}
		}
	}

	@Override
	public String getStrategyName() {
		return "Individual Task Restart";
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RestartAllStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public RestartIndividualStrategy create(ExecutionGraph executionGraph) {
			return new RestartIndividualStrategy(executionGraph);
		}
	}
}
