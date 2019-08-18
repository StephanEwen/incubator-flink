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

import org.apache.flink.impl.connector.source.RecordsWithSplitIds;
import org.apache.flink.impl.connector.source.mocks.MockSplitReader;
import org.apache.flink.impl.connector.source.mocks.MockSplit;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.testutils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SplitFetcher}
 */
public class SplitFetcherTest {
	private AtomicReference<Throwable> exception;
	private ThrowableCatchingRunnableWrapper wrapper;
	private MockSplitReader mockSplitReader;
	private BlockingQueue<RecordsWithSplitIds<int[]>> outputQueue;
	private SplitFetcher<int[], MockSplit> fetcher;
	Thread fetcherThread;

	@Before
	public void setup() {
		exception = new AtomicReference<>();
		wrapper = new ThrowableCatchingRunnableWrapper(t -> exception.set(t));
	}

	@After
	public void tearDown() {
		if (fetcher != null) {
			fetcher.shutdown();
		}
	}

	@Test (timeout = 30000L)
	public void testBlockOnEmptySplits() throws InterruptedException {
		setupFetcher(1, true, true);
		MockSplit mockSplit = new MockSplit(0);
		mockSplit.addRecord(123);
		// Add a split to the fetcher to trigger actual fetch.
		fetcher.addSplits(Collections.singletonList(mockSplit));
		// The fetcher should simply wait if there is no assignment.
		Collection<int[]> records = outputQueue.take().recordsBySplits().get("0");
		assertArrayEquals(new int[]{123, 0}, records.iterator().next());
	}

	@Test (timeout = 30000L)
	public void testAddSplitsAndFetchRecords() throws InterruptedException {
		setupFetcher(1, true, true);
		MockSplit mockSplit = new MockSplit(0);
		mockSplit.addRecord(123);
		// Add a split to the fetcher to trigger actual fetch.
		fetcher.addSplits(Collections.singletonList(mockSplit));
		// Blocking util get the first element out of the queue and verify it.
		Collection<int[]> records = outputQueue.take().recordsBySplits().get("0");
		assertArrayEquals(new int[]{123, 0}, records.iterator().next());
	}

	@Test (timeout = 30000L)
	public void testRemoveSplits() throws InterruptedException {
		setupFetcher(1, true, true);
		MockSplit mockSplit = new MockSplit(0);
		mockSplit.addRecord(123);
		// Add a split to the fetcher to trigger actual fetch.
		fetcher.addSplits(Collections.singletonList(mockSplit));
		// Blocking util get the first element out of the queue and verify it.
		Collection<int[]> records = outputQueue.take().recordsBySplits().get("0");
		assertArrayEquals(new int[]{123, 0}, records.iterator().next());
		fetcher.removeSplits(Collections.singletonList(mockSplit));
		// the running task should become null if there is no task to run.
		TestUtils.waitUntil(() -> fetcherThread.getState() == Thread.State.WAITING);
		assertTrue(fetcher.isIdle());
	}

	/**
	 * This test is more of a race condition test. It lets the split fetcher run and
	 * keep waking it up. The SplitReader should never be interrupted. Instead
	 * only the {@link SplitReader#wakeUp()} method should be invoked.
	 */
	@Test
	public void testNeverInterruptSplitReader() throws InterruptedException {
		setupFetcher(1, false, true);
		MockSplit mockSplit = new MockSplit(0);
		mockSplit.addRecord(123);
		// Consume a record to ensure the split fetcher is running normally.
		fetcher.addSplits(Collections.singletonList(mockSplit));
		Collection<int[]> records = outputQueue.take().recordsBySplits().get("0");
		assertArrayEquals(new int[]{123, 0}, records.iterator().next());

		// Interrupt the fetcher thread 10K times.
		for (int i = 0; i < 10000; i++) {
			fetcher.wakeUp(false);
		}
		fetcher.shutdown();
		fetcherThread.join();
		assertNull("The split reader should never be interrupted.", exception.get());
	}

	// -----------------------------------

	private void setupFetcher(int numRecordsPerSplitPerFetch,
							  boolean blockingFetch,
							  boolean handleSplitsInOneShot) {

		mockSplitReader = new MockSplitReader(numRecordsPerSplitPerFetch,
											  blockingFetch,
											  handleSplitsInOneShot);
		outputQueue = new LinkedBlockingQueue<>();
		fetcher = new SplitFetcher<>(0,
									 outputQueue,
									 mockSplitReader,
									 () -> {});
		fetcherThread = runFetcher(fetcher);
	}

	private Thread runFetcher(SplitFetcher<int[], MockSplit> splitFetcher) {
		Thread t = new Thread(wrapper.wrap(splitFetcher));
		t.start();
		return t;
	}
}
