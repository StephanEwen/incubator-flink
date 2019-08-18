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

package org.apache.flink.impl.connector.source;

import org.apache.flink.api.connectors.source.Boundedness;
import org.apache.flink.api.connectors.source.SourceOutput;
import org.apache.flink.api.connectors.source.SourceReader;
import org.apache.flink.impl.connector.source.mocks.MockSourceReader;
import org.apache.flink.impl.connector.source.mocks.MockSplit;
import org.apache.flink.impl.connector.source.mocks.MockSplitReader;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.splitreader.SplitsChange;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.impl.connector.source.synchronization.FutureNotifier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * A unit test class for {@link SourceReaderBase}
 */
public class SourceReaderBaseTest {
	private final int NUM_SPLITS = 10;
	private final int NUM_RECORDS_PER_SPLIT = 10;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	/**
	 * Simply test the reader reads all the splits fine.
	 */
	@Test (timeout = 30000L)
	public void testRead() {
		MockSourceReader reader = createReader(Boundedness.BOUNDED);
		reader.addSplits(getMockSplits(Boundedness.BOUNDED));
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		while (output.count < 100) {
			reader.pollNext(output);
		}
		output.validate();
	}

	@Test
	public void testAddSplitToExistingFetcher() {
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		// Add a split to start the fetcher.
		List<MockSplit> splits = Collections.singletonList(getMockSplit(0, Boundedness.BOUNDED));
		// Poll 5 records and let it block on the element queue which only have capacity fo 1;
		MockSourceReader reader = consumeRecords(splits, output, 5, Boundedness.BOUNDED);
		List<MockSplit> newSplits = new ArrayList<>();
		for (int i = 1; i < NUM_SPLITS; i++) {
			newSplits.add(getMockSplit(i, Boundedness.BOUNDED));
		}
		reader.addSplits(newSplits);

		while (output.count() < 100) {
			reader.pollNext(output);
		}
		output.validate();
	}

	@Test (timeout = 30000L)
	public void testPollingFromEmptyQueue() {
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		List<MockSplit> splits = Collections.singletonList(getMockSplit(0, Boundedness.BOUNDED));
		// Consumer all the records in the s;oit.
		MockSourceReader reader = consumeRecords(splits, output, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED);
		// Now let the main thread poll again.
		assertEquals("The status should be ", SourceReader.Status.AVAILABLE_LATER, reader.pollNext(output));
	}

	@Test (timeout = 30000L)
	public void testAvailableOnEmptyQueue() throws ExecutionException, InterruptedException {
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		List<MockSplit> splits = Collections.singletonList(getMockSplit(0, Boundedness.BOUNDED));
		// Consumer all the records in the split.
		MockSourceReader reader = consumeRecords(splits, output, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED);

		CompletableFuture<?> future = reader.available();
		assertFalse("There should be no records ready for poll.", future.isDone());
		// Add a split to the reader so there are more records to be read.
		reader.addSplits(Collections.singletonList(getMockSplit(1, Boundedness.BOUNDED)));
		// THe future should be completed fairly soon. Otherwise the test will hit timeout and fail.
		future.get();
	}

	@Test (timeout = 30000L)
	public void testSnapshot() {
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		// Add a split to start the fetcher.
		List<MockSplit> splits = getMockSplits(Boundedness.BOUNDED);
		// Poll 5 records. That means split 0 and 1 will at index 2, split 1 will at index 1.
		MockSourceReader reader = consumeRecords(splits, output, 5, Boundedness.BOUNDED);

		List<MockSplit> state = reader.snapshotState();
		assertEquals("The snapshot should only have 10 splits. ", 10, state.size());
		for (int i = 0; i < 2; i++) {
			assertEquals("The first four splits should have been fully consumed.", 2, state.get(i).index());
		}
		assertEquals("The fourth split should have been consumed 5 elements.", 1, state.get(2).index());
		for (int i = 3; i < 10; i++) {
			assertEquals("The last 5 splits should have not been consumed.", 0, state.get(i).index());
		}
	}

	@Test
	public void testExceptionInSplitReader() throws InterruptedException {
		expectedException.expect(RuntimeException.class);
		expectedException.expectMessage("One or more fetchers have encountered exception");
		final String errMsg = "Testing Exception";

		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		MockSourceReader reader = new MockSourceReader(
				futureNotifier,
				elementsQueue,
				() -> new SplitReader<int[], MockSplit>() {
					@Override
					public RecordsWithSplitIds<int[]> fetch() {
						throw new RuntimeException(errMsg);
					}

					@Override
					public void handleSplitsChanges(Queue<SplitsChange<MockSplit>> splitsChanges) {
						// We have to handle split changes first, otherwise fetch will not be called.
						splitsChanges.clear();
					}

					@Override
					public void wakeUp() {}

					@Override
					public void configure(Configuration config) {}
				});


		reader.configure(getConfig(Boundedness.BOUNDED));
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		reader.addSplits(Collections.singletonList(getMockSplit(0, Boundedness.UNBOUNDED)));
		// This is not a real infinite loop, it is supposed to throw exception after two polls.
		while (true) {
			reader.pollNext(output);
			// Add a sleep to avoid tight loop.
			Thread.sleep(1);
		}
	}

	// ---------------- helper methods -----------------

	private MockSourceReader createReader(Boundedness boundedness) {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		MockSplitReader mockSplitReader =
				new MockSplitReader(2, true, true);
		MockSourceReader reader = new MockSourceReader(
				futureNotifier,
				elementsQueue,
				() -> mockSplitReader);
		Configuration config = getConfig(boundedness);
		reader.configure(config);
		return reader;
	}

	private MockSourceReader consumeRecords(List<MockSplit> splits,
											   ValidatingSourceOutput output,
											   int n,
											   Boundedness boundedness) {
		MockSourceReader reader = createReader(boundedness);
		// Add splits to start the fetcher.
		reader.addSplits(splits);
		// Poll all the n records of the single split.
		while (output.count() < n) {
			reader.pollNext(output);
		}
		return reader;
	}

	private List<MockSplit> getMockSplits(Boundedness boundedness) {
		List<MockSplit> mockSplits = new ArrayList<>();
		for (int i = 0; i < NUM_SPLITS; i++) {
			mockSplits.add(getMockSplit(i, boundedness));
		}
		return mockSplits;
	}

	private MockSplit getMockSplit(int splitId, Boundedness boundedness) {

		MockSplit mockSplit;
		if (boundedness == Boundedness.BOUNDED) {
			mockSplit = new MockSplit(splitId, 0, NUM_RECORDS_PER_SPLIT);
		} else {
			mockSplit = new MockSplit(splitId);
		}
		for (int j = 0; j < NUM_RECORDS_PER_SPLIT; j++) {
			mockSplit.addRecord(splitId * 10 + j);
		}
		return mockSplit;
	}

	private Configuration getConfig(Boundedness boundedness) {
		Configuration config = new Configuration();
		config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
		config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
		config.setString(SourceReaderOptions.SOURCE_READER_BOUNDEDNESS, boundedness.name());
		return config;
	}

	// ---------------- helper classes -----------------

	/**
	 * A source output that validates the output.
	 */
	private static class ValidatingSourceOutput implements SourceOutput<Integer> {
		private Set<Integer> consumedValues = new HashSet<>();
		private int max = Integer.MIN_VALUE;
		private int min = Integer.MAX_VALUE;

		private int count = 0;

		@Override
		public void collect(Integer element) {
			max = Math.max(element, max);
			min = Math.min(element, min);
			count++;
			consumedValues.add(element);
		}

		@Override
		public void collect(Integer element, Long timestamp) {
			collect(element);
		}

		public void validate() {
			assertEquals("Should be 100 distinct elements in total", 100, consumedValues.size());
			assertEquals("Should be 100 elements in total", 100, count);
			assertEquals("The min value should be 0", 0, min);
			assertEquals("The max value should be 99", 99, max);
		}
		public int count() {
			return count;
		}

	}
}
