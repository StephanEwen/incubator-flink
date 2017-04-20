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

import org.junit.Test;

/**
 * Tests that make sure that the building of pipelined connected failover regions works
 * correctly.
 */
public class PipelinedFailoverRegionBuildingTest {

	/**
	 * Tests that validates that a graph with single unconnected vertices works correctly.
	 */
	@Test
	public void testIndividualVertices() throws Exception {
		
	}

	/**
	 * Tests that validates that embarrassingly parallel chains of vertices work correctly. 
	 */
	@Test
	public void testEmbarrassinglyParallelCase() throws Exception {

	}

	/**
	 * Tests that validates that a single pipelined component via a sequence of all-to-all
	 * connections works correctly.
	 */
	@Test
	public void testOneComponentViaTwoExchanges() throws Exception {

	}

	/**
	 * Tests that validates that a single pipelined component via a cascade of joins
	 * works correctly.
	 */
	@Test
	public void testOneComponentViaCascadeOfJoins() throws Exception {

	}

	@Test
	public void testTwoComponentsViaBlockingExchange() throws Exception {

	}

	@Test
	public void testMultipleComponentsViaCascadeOfJoins() throws Exception {

	}

	@Test
	public void testDiamondWithMixedPipelinedAndBlockingExchanges() throws Exception {

	}
}
