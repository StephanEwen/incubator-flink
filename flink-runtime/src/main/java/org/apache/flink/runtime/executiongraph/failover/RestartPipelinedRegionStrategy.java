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

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that restarts regions of the ExecutionGraph. A region is defined
 * by this strategy as the weakly connected component of tasks that communicate via pipelined
 * data exchange.
 */
public class RestartPipelinedRegionStrategy extends FailoverStrategy {

	private final ExecutionGraph executionGraph;

	public RestartPipelinedRegionStrategy(ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
		throw new UnsupportedOperationException("not yet implemented");
	}

	// ------------------------------------------------------------------------
	//  failover implementation
	// ------------------------------------------------------------------------ 

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		// to be implemented
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// TODO - build up the failover regions, etc.
	}

	@Override
	public String getStrategyName() {
		return "Pipelined Region Failover";
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
