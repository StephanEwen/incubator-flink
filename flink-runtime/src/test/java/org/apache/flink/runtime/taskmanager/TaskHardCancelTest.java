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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestJvmProcess;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This test tests the hard process failure is a task does not cancel within
 * the cancellation timeout.
 */
public class TaskHardCancelTest {

	@Test
	public void testKillProcessWhenTaskCancellationFails() throws Exception {
		// validate the test integrity
		Task task = TaskTestUtils.createTask(NoOpInvokable.class.getName());
		task.startTaskThread();
		task.getExecutingThread().join();
		assertEquals(ExecutionState.FINISHED, task.getExecutionState());

		TaskProcess process = new TaskProcess();
		process.startProcess();
		process.waitFor();
	}

	// ------------------------------------------------------------------------

	/** Test task that cannot be cancelled. */
	public static final class NonInterruptibleTask extends AbstractInvokable {

		static final OneShotLatch IN_INVOKE = new OneShotLatch();

		public NonInterruptibleTask(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {
			IN_INVOKE.trigger();
			CommonTestUtils.blockForeverNonInterruptibly();
		}

		@Override
		public void cancel() {}
	}

	// ------------------------------------------------------------------------

	/**
	 * An external process running the task code.
	 */
	public static final class TaskProcess extends TestJvmProcess {

		public TaskProcess() throws Exception {}

		@Override
		public String getName() {
			return "Task Process";
		}

		@Override
		public String[] getJvmArgs() {
			return new String[0];
		}

		@Override
		public String getEntryPointClassName() {
			return getClass().getName();
		}

		public static void main(String[] args) throws Exception {
			try {
				final Configuration config = new Configuration();
				config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, 10);
				config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 100);

				Task task = TaskTestUtils.createTask(NonInterruptibleTask.class.getName(), config);
				task.startTaskThread();

				NonInterruptibleTask.IN_INVOKE.await();
				task.cancelExecution();
			}
			catch (Throwable t) {
				// in case of an error, the process must not go away
				CommonTestUtils.blockForeverNonInterruptibly();
			}
		}
	}
}
