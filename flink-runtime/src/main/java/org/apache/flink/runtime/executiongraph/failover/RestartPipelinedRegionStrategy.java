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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that restarts regions of the ExecutionGraph. A region is defined
 * by this strategy as the weakly connected component of tasks that communicate via pipelined
 * data exchange.
 */
public class RestartPipelinedRegionStrategy extends FailoverStrategy {

	/** The log object used for debugging. */
	static final Logger LOG = LoggerFactory.getLogger(RestartPipelinedRegionStrategy.class);

	private final ExecutionGraph executionGraph;

	private final List<FailoverRegion> failoverRegions;

	public RestartPipelinedRegionStrategy(ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
		this.failoverRegions = new LinkedList<>();
	}

	// ------------------------------------------------------------------------
	//  failover implementation
	// ------------------------------------------------------------------------ 

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		// to be implemented
		ExecutionVertex ev = taskExecution.getVertex();
		FailoverRegion failoverRegion = getFailoverRegion(ev);
		if (failoverRegion == null) {
			executionGraph.failGlobal(new FlinkException("Can not find a failover region for the execution " + ev.getTaskNameWithSubtaskIndex()));
		}
		else {
			failoverRegion.onExecutionFail(ev, cause);
		}
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// TODO - build up the failover regions, etc.
		LOG.info("begin to generateAllFailoverRegion {}", newJobVerticesTopological.size());
		generateAllFailoverRegion(newJobVerticesTopological);
	}

	@Override
	public String getStrategyName() {
		return "Pipelined Region Failover";
	}

	// find the failover region that contains the execution vertex
	@VisibleForTesting
	public FailoverRegion getFailoverRegion(ExecutionVertex ev) {
		FailoverRegion failoverRegion = null;
		Iterator<FailoverRegion> regionIterator = failoverRegions.iterator();
		while (regionIterator.hasNext()) {
			FailoverRegion region = regionIterator.next();
			if (region.containsExecution(ev)) {
				failoverRegion = region;
				break;
			}
		}
		return failoverRegion;
	}

	// Generate all the FailoverRegion from the new added job vertexes
	private void generateAllFailoverRegion(List<ExecutionJobVertex> newJobVerticesTopological) {
		for (ExecutionJobVertex ejv : newJobVerticesTopological) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				if (getFailoverRegion(ev) != null) {
					continue;
				}
				List<ExecutionVertex> pipelinedExecutions = new LinkedList<>();
				List<ExecutionVertex> orgExecutions = new LinkedList<>();
				orgExecutions.add(ev);
				pipelinedExecutions.add(ev);
				getAllPipelinedConnectedVertexes(orgExecutions, pipelinedExecutions);
				failoverRegions.add(new FailoverRegion(executionGraph, pipelinedExecutions));
			}
		}
	}

	/**
	 * Get all connected executions of the original executions
	 *
	 * @param orgExecutions  the original execution vertexes
	 * @param connectedExecutions  the total connected executions
	 */
	private static void getAllPipelinedConnectedVertexes(List<ExecutionVertex> orgExecutions, List<ExecutionVertex> connectedExecutions) {
		List<ExecutionVertex> newAddedExecutions = new LinkedList<>();
		for (ExecutionVertex ev : orgExecutions) {
			// Add downstream ExecutionVertex
			for (IntermediateResultPartition irp : ev.getProducedPartitions().values()) {
				if (irp.getIntermediateResult().getResultType().isPipelined()) {
					for (List<ExecutionEdge> consumers : irp.getConsumers()) {
						for (ExecutionEdge consumer : consumers) {
							ExecutionVertex cev = consumer.getTarget();
							if (!connectedExecutions.contains(cev)) {
								newAddedExecutions.add(cev);
							}
						}
					}
				}
			}
			if (!newAddedExecutions.isEmpty()) {
				connectedExecutions.addAll(newAddedExecutions);
				getAllPipelinedConnectedVertexes(newAddedExecutions, connectedExecutions);
				newAddedExecutions.clear();
			}
			// Add upstream ExecutionVertex
			int inputNum = ev.getNumberOfInputs();
			for (int i = 0; i < inputNum; i++) {
				for (ExecutionEdge input : ev.getInputEdges(i)) {
					if (input.getSource().getIntermediateResult().getResultType().isPipelined()) {
						ExecutionVertex pev = input.getSource().getProducer();
						if (!connectedExecutions.contains(pev)) {
							newAddedExecutions.add(pev);
						}
					}
				}
			}
			if (!newAddedExecutions.isEmpty()) {
				connectedExecutions.addAll(0, newAddedExecutions);
				getAllPipelinedConnectedVertexes(newAddedExecutions, connectedExecutions);
				newAddedExecutions.clear();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RestartPipelinedRegionStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RestartPipelinedRegionStrategy(executionGraph);
		}
	}
}
