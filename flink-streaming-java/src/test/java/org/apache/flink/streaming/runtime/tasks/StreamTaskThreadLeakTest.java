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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test checks that no threads remain once the stream task is disposed.
 */
public class StreamTaskThreadLeakTest extends TestLogger {

	private Set<Thread> preExistingThreads;

	// ------------------------------------------------------------------------
	//  thread checks
	// ------------------------------------------------------------------------

	@Before
	public void collectPreTestThreads() {
		preExistingThreads = Collections.unmodifiableSet(new HashSet<>(getCurrentThreads()));
	}

	@After
	public void validateNoLingeringThreads() {
		HashSet<Thread> threads = new HashSet<>(getCurrentThreads());
		threads.removeAll(preExistingThreads);
		if (threads.size() > 0) {
			fail("Lingering threads: " + threads);
		}
	}

	static Collection<Thread> getCurrentThreads() {
		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);
		return Arrays.asList(threads);
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	/**
	 * Tests that the stream task releases all threads created in the constructor in the
	 * disposal method.
	 */
	@Test
	public void testStreamTaskCanceledBeforeInvoke() throws Exception {

		// we use the test harness here only as a factory for the task operator chain and output writers
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
				TestStreamTask::new,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setBufferTimeout(1);

		StreamMockEnvironment env = testHarness.createEnvironment();
		env.addOutput(Collections.emptyList(), StringSerializer.INSTANCE);

		// create the task the regular way, so that we can simulate the case where
		// the 'invoke()' method is never called

		final StreamTask<?, ?> task = new TestStreamTask(env);

		// guard the test, check that this in fact started the output flusher
		boolean foundFlusher = false;
		for (Thread t : getCurrentThreads()) {
			if (t.getName().startsWith("OutputFlusher")) {
				foundFlusher = true;
				break;
			}
		}

		assertTrue("test inconclusive: output flusher not started", foundFlusher);

		// this must ensure that all resources from the constructor are released
		task.dispose();
	}

	@Test
	public void testStreamTaskRegularLifeCycle() throws Exception {

		OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
				OneInputStreamTask::new,
				1, 1,
				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamMap<String, String> mapOperator = new StreamMap<>(v -> v);
		streamConfig.setStreamOperator(mapOperator);
		streamConfig.setOperatorID(new OperatorID());

		// start the task
		testHarness.invoke();
		testHarness.waitForTaskRunning();

		// cancel and shutdown
		testHarness.endInput();
		testHarness.waitForTaskCompletion();
	}

	private static class TestStreamTask extends StreamTask<String, StreamOperator<String>> {

		TestStreamTask(Environment env) {
			super(env);
		}

		@Override
		protected void init() {}

		@Override
		protected void run() {}

		@Override
		protected void cleanup() {}

		@Override
		protected void cancelTask() {}
	}
}
