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

package org.apache.flink.impl.connector.source.fetcher;

import org.apache.flink.api.connectors.source.Boundedness;
import org.apache.flink.impl.connector.source.mocks.MockSplit;
import org.apache.flink.impl.connector.source.mocks.MockSplitReader;
import org.junit.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;

/**
 * Unit test for {@link FetchTask}
 */
public class FetchTaskTest {

	/**
	 * The FetchTask.wakeUp() should never interrupt the SplitReader. Instead, it should always call
	 * SplitReader.wakeUp().
	 */
	@Test
	public void testWakeUpNeverInterruptSplitReader() throws InterruptedException {
		MockSplitReader mockSplitReader = new MockSplitReader(
				1, false, true);
		LoopingRunnable loopingRunnable = new LoopingRunnable();
		AtomicReference<Throwable> exception = new AtomicReference<>();
		ThrowableCatchingRunnableWrapper wrapper = new ThrowableCatchingRunnableWrapper(exception::set);
		Thread t = new Thread(wrapper.wrap(loopingRunnable));

		FetchTask<int[], MockSplit> fetchTask =
				new FetchTask<>(mockSplitReader, new LinkedBlockingQueue<>(), s -> {}, t);
		loopingRunnable.wrapped = fetchTask;
		t.start();
		for (int i = 0; i < 10000; i++) {
			fetchTask.wakeUp();
		}
		loopingRunnable.closed = true;
		t.join();
		assertNull(exception.get());
	}

	private static class LoopingRunnable implements Runnable {
		private FetchTask wrapped = null;
		private volatile boolean closed = false;

		@Override
		public void run() {
			while (!closed) {
				try {
					wrapped.run();
				} catch (InterruptedException e) {
					// Ignore the interrupted exception. A failing exception should be a RuntimeException
					// thrown by the MockSplitReader.
				}
			}
		}
	}
}
