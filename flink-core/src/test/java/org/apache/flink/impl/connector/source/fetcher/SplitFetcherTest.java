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

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.RecordsBySplits;
import org.apache.flink.impl.connector.source.RecordsWithSplitIds;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.splitreader.SplitsChange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SplitFetcher}
 */
public class SplitFetcherTest {
	private AtomicReference<Throwable> exception;
	private ThrowableCatchingRunnableWrapper wrapper;
	private BlockingQueue<Integer> sourceQueue = new LinkedBlockingQueue<>();
	private PassThroughReader passThroughReader = new PassThroughReader(sourceQueue);
	private BlockingQueue<RecordsWithSplitIds<Integer>> outputQueue = new LinkedBlockingQueue<>();
	private SplitFetcher<Integer, TestingSplit> fetcher;
	Thread fetcherThread;

	@Before
	public void setup() {
		exception = new AtomicReference<>();
		wrapper = new ThrowableCatchingRunnableWrapper(t -> exception.set(t));
		sourceQueue = new LinkedBlockingQueue<>();
		passThroughReader = new PassThroughReader(sourceQueue);
		outputQueue = new LinkedBlockingQueue<>();
		fetcher = new SplitFetcher<>(0,
									 outputQueue,
									 passThroughReader,
									 s -> {},
									 () -> {});
		fetcherThread = runFetcher(fetcher);
	}

	@After
	public void tearDown() {
		fetcher.shutdown();
	}

	@Test (timeout = 30000L)
	public void testBlockOnEmptySplits() throws InterruptedException {
		sourceQueue.offer(123);
		// The fetcher should simply wait if there is no assignment.
		waitUntil(() -> fetcherThread.getState() == Thread.State.WAITING);
		assertTrue(!sourceQueue.isEmpty());
	}

	@Test (timeout = 30000L)
	public void testAddSplitsAndFetchRecords() throws InterruptedException {
		sourceQueue.offer(123);
		// Assign a split to the fetcher to let it start fetching.
		fetcher.addSplits(Collections.singletonList(new TestingSplit(0)));
		// Blocking util get the first element out of the queue and verify it.
		assertEquals(Collections.singletonList(123), outputQueue.take().recordsBySplits().get("0"));
	}

	@Test (timeout = 30000L)
	public void testRemoveSplits() throws InterruptedException {
		sourceQueue.offer(123);
		// Assign a split to the fetcher to let it start fetching.
		fetcher.addSplits(Collections.singletonList(new TestingSplit(0)));
		// Blocking util get the first element out of the queue and verify it.
		assertEquals(Collections.singletonList(123), outputQueue.take().recordsBySplits().get("0"));
		fetcher.removeSplits(Collections.singletonList(new TestingSplit(0)));
		// the running task should become null if there is no task to run.
		waitUntil(() -> fetcherThread.getState() == Thread.State.WAITING);
		assertNull(fetcher.runningTask());
		assertFalse(fetcher.shouldRunFetchTask());
	}

	/**
	 * This test is more of a race condition test. It lets the split fetcher run and
	 * keep waking it up. The SplitReader should never be interrupted. Instead
	 * only the {@link SplitReader#wakeUp()} method should be invoked.
	 */
	@Test
	public void testNeverInterruptSplitReader() throws InterruptedException {
		// Consume a record to ensure the split fetcher is running normally.
		fetcher.addSplits(Collections.singletonList(new TestingSplit(0)));
		sourceQueue.offer(123);
		assertEquals(Collections.singletonList(123), outputQueue.take().recordsBySplits().get("0"));

		// Interrupt the fetcher thread 10K times.
		for (int i = 0; i < 10000; i++) {
			fetcher.wakeUp(false);
		}
		// The split reader should never be interrupted.
		assertNull(exception.get());
		// SplitReader.fetch() should have run at least 10 times.
		System.out.println(passThroughReader.invocationCount.get());
	}

	// -----------------------------------

	private Thread runFetcher(SplitFetcher<Integer, TestingSplit> splitFetcher) {
		Thread t = new Thread(wrapper.wrap(splitFetcher));
		t.start();
		return t;
	}

	private void waitUntil(Supplier<Boolean> action) throws InterruptedException {
		while (!action.get()) {
			Thread.sleep(1);
		}
	}

	// -----------------------------------

	private static class PassThroughReader implements SplitReader<Integer, TestingSplit> {
		private final Integer WAKE_UP = new Integer(-1);
		private final BlockingQueue<Integer> sourceQueue;
		private final AtomicLong invocationCount;

		private PassThroughReader(BlockingQueue<Integer> sourceQueue) {
			this.sourceQueue = sourceQueue;
			this.invocationCount = new AtomicLong(0);
		}

		@Override
		public void fetch(BlockingQueue<RecordsWithSplitIds<Integer>> queue,
						  Consumer<String> splitFinishedCallback) throws InterruptedException {
			invocationCount.incrementAndGet();

			if (Thread.currentThread().isInterrupted()) {
				throw new RuntimeException("Should never be interrupted.");
			}

			Integer value = sourceQueue.poll();
			if (value != null && value != WAKE_UP) {
				RecordsBySplits<Integer> recordsBySplits = new RecordsBySplits<>();
				recordsBySplits.add("0", value);
				queue.offer(recordsBySplits);
			}
		}

		@Override
		public void handleSplitsChanges(Queue<SplitsChange<TestingSplit>> splitsChanges) {
			splitsChanges.clear();
		}

		public void wakeUp() {
			sourceQueue.offer(WAKE_UP);
		}

		@Override
		public void configure(Configuration config) {

		}
	}

	private static class TestingSplit implements SourceSplit {
		private final int id;

		TestingSplit(int id) {
			this.id = id;
		}

		@Override
		public String splitId() {
			return Integer.toString(id);
		}
	}
}
