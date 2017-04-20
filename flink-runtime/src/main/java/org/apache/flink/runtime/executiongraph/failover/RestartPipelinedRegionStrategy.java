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
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that restarts regions of the ExecutionGraph. A region is defined
 * by this strategy as the weakly connected component of tasks that communicate via pipelined
 * data exchange.
 */
public class RestartPipelinedRegionStrategy extends FailoverStrategy {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(RestartPipelinedRegionStrategy.class);

	private final ExecutionGraph executionGraph;

	private final HashMap<ExecutionVertex, FailoverRegion> vertexToRegion;

	public RestartPipelinedRegionStrategy(ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
		this.vertexToRegion = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	//  failover implementation
	// ------------------------------------------------------------------------ 

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		final ExecutionVertex ev = taskExecution.getVertex();
		final FailoverRegion failoverRegion = vertexToRegion.get(ev);

		if (failoverRegion == null) {
			executionGraph.failGlobal(new FlinkException(
					"Can not find a failover region for the execution " + ev.getTaskNameWithSubtaskIndex()));
		}
		else {
			failoverRegion.onExecutionFail(ev, cause);
		}
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		LOG.debug("Generating failover regions for {} new job vertices", newJobVerticesTopological.size());
		generateAllFailoverRegion(newJobVerticesTopological);
	}

	@Override
	public String getStrategyName() {
		return "Pipelined Region Failover";
	}

	/**
	 * Generate all the FailoverRegion from the new added job vertexes
 	 */
	private void generateAllFailoverRegion(List<ExecutionJobVertex> newJobVerticesTopological) {
		final HashMap<ExecutionVertex, List<ExecutionVertex>> vertexToPipelined = new HashMap<>();
		final IdentityHashMap<List<ExecutionVertex>, Object> distinctRegions = new IdentityHashMap<>();

		// this loop will worst case iterate over every edge in the graph (complexity is O(#edges))

		for (ExecutionJobVertex ejv : newJobVerticesTopological) {

			// see if this one has pipelined inputs at all
			final List<IntermediateResult> inputs = ejv.getInputs();
			final int numInputs = inputs.size();
			boolean hasPipelinedInputs = false;

			for (IntermediateResult input : inputs) {
				if (input.getResultType().isPipelined()) {
					hasPipelinedInputs = true;
					break;
				}
			}

			if (hasPipelinedInputs) {
				// build upon the predecessors
				for (ExecutionVertex ev : ejv.getTaskVertices()) {

					// remember the region in which we are
					List<ExecutionVertex> thisRegion = null;

					for (int inputNum = 0; inputNum < numInputs; inputNum++) {
						if (inputs.get(inputNum).getResultType().isPipelined()) {

							for (ExecutionEdge edge : ev.getInputEdges(inputNum)) {
								final ExecutionVertex predecessor = edge.getSource().getProducer();
								final List<ExecutionVertex> predecessorRegion = vertexToPipelined.get(predecessor);

								if (thisRegion != null) {
									// we already have a region. see if it is the same as the predecessor's region
									if (predecessorRegion != thisRegion) {

										// we need to merge our region and the predecessor's region
										thisRegion.addAll(predecessorRegion);
										distinctRegions.remove(predecessorRegion);

										// remap the vertices from that merged region
										for (ExecutionVertex inPredRegion: predecessorRegion) {
											vertexToPipelined.put(inPredRegion, thisRegion);
										}
									}
								}
								else {
									// first case, make this our region
									thisRegion = predecessorRegion;
									thisRegion.add(ev);
									vertexToPipelined.put(ev, thisRegion);
								}
							}
						}
					}
				}
			}
			else {
				// first one, gets an individual region
				for (ExecutionVertex ev : ejv.getTaskVertices()) {
					ArrayList<ExecutionVertex> region = new ArrayList<>(1);
					region.add(ev);
					vertexToPipelined.put(ev, region);
					distinctRegions.put(region, null);
				}
			}
		}

		// now that we have all regions, create the failover region objects 
		for (List<ExecutionVertex> region : distinctRegions.keySet()) {
			final FailoverRegion failoverRegion = new FailoverRegion(executionGraph, region);
			for (ExecutionVertex ev : region) {
				vertexToRegion.put(ev, failoverRegion);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  testing
	// ------------------------------------------------------------------------

	/**
	 * Finds the failover region that contains the given execution vertex.
 	 */
	@VisibleForTesting
	public FailoverRegion getFailoverRegion(ExecutionVertex ev) {
		return vertexToRegion.get(ev);
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
