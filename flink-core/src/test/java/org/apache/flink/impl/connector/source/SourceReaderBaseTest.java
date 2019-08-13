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

import org.apache.flink.api.connectors.source.SourceOutput;
import org.apache.flink.api.connectors.source.SourceReader;
import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.splitreader.SplitReader;
import org.apache.flink.api.connectors.source.splitreader.SplitsChange;
import org.apache.flink.api.connectors.source.splitreader.SplitsChangesWithEpoch;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
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

	/**
	 * Simply test the reader reads all the splits fine.
	 */
	@Test (timeout = 30000L)
	public void testRead() {
		TestingSourceReader reader = createReader();
		List<IdAndIndex> splits = new ArrayList<>();
		for (int i = 0; i < NUM_SPLITS; i++) {
			splits.add(new IdAndIndex(i, 0));
		}
		reader.addSplits(splits);
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
		List<IdAndIndex> splits = Collections.singletonList(new IdAndIndex(0, 0));
		// Poll 5 records and let it block on the element queue which only have capacity fo 1;
		TestingSourceReader reader = consumeRecords(splits, output, 5);
		System.out.println("Adding splits");
		List<IdAndIndex> newSplits = new ArrayList<>();
		for (int i = 1; i < NUM_SPLITS; i++) {
			newSplits.add(new IdAndIndex(i, 0));
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
		List<IdAndIndex> splits = Collections.singletonList(new IdAndIndex(0, 0));
		// Consumer all the records in the s;oit.
		TestingSourceReader reader = consumeRecords(splits, output, NUM_RECORDS_PER_SPLIT);
		// Now let the main thread poll again.
		assertEquals("The status should be ", SourceReader.Status.AVAILABLE_LATER, reader.pollNext(output));
	}

	@Test (timeout = 30000L)
	public void testAvailable() throws ExecutionException, InterruptedException {
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		List<IdAndIndex> splits = Collections.singletonList(new IdAndIndex(0, 0));
		// Consumer all the records in the s;oit.
		TestingSourceReader reader = consumeRecords(splits, output, NUM_RECORDS_PER_SPLIT);

		CompletableFuture<?> future = reader.available();
		assertFalse("There should be no records read for poll.", future.isDone());
		// Add a split to the reader so there are more records to be read.
		reader.addSplits(Collections.singletonList(new IdAndIndex(1, 0)));
		// THe future should be completed fairly soon. Otherwise the test will hit timeout and fail.
		future.get();
	}

	@Test (timeout = 30000L)
	public void testSnapshot() {
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		// Add a split to start the fetcher.
		List<IdAndIndex> splits = new ArrayList<>();
		for (int i = 0; i < NUM_SPLITS; i++) {
			splits.add(new IdAndIndex(i, 0));
		}
		// Poll 5 records and let it block on the element queue which only have capacity fo 1;
		TestingSourceReader reader = consumeRecords(splits, output, 45);

		List<IdAndIndex> state = reader.snapshotState();
		assertEquals("The snapshot should only have 10 splits. ", 10, state.size());
		for (int i = 0; i < 4; i++) {
			assertEquals("The first four splits should have been fully consumed.", 9, state.get(i).idx);
		}
		assertEquals("The fourth split should have been consumed 5 elements.", 4, state.get(4).idx);
		for (int i = 5; i < 10; i++) {
			assertEquals("The last 5 splits should have not been consumed.", 0, state.get(i).idx);
		}
	}

	// ---------------- helper methods -----------------

	private TestingSourceReader createReader() {
		Configuration config = new Configuration();
		config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
		config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
		config.setString(SourceReaderOptions.SOURCE_READER_BOUNDEDNESS, "bounded");

		List<int[]> records = new ArrayList<>();
		for (int i = 0; i < NUM_SPLITS; i++) {
			int[] split = new int[NUM_RECORDS_PER_SPLIT];
			records.add(split);
			for (int j = 0; j < NUM_RECORDS_PER_SPLIT; j++) {
				split[j] = i * 10 + j;
			}
		}
		TestingSourceReader reader = new TestingSourceReader(records);
		reader.configure(config);
		return reader;
	}

	private TestingSourceReader consumeRecords(List<IdAndIndex> splits, ValidatingSourceOutput output, int n) {
		TestingSourceReader reader = createReader();
		// Add splits to start the fetcher.
		reader.addSplits(splits);
		// Poll all the n records of the single split.
		while (output.count() < n) {
			reader.pollNext(output);
		}
		return reader;
	}

	// ---------------- helper classes -----------------

	/**
	 * A testing split class.
	 */
	private static final class IdAndIndex implements SourceSplit {
		public final int id;
		public final int idx;

		IdAndIndex(int id, int idx) {
			this.id = id;
			this.idx = idx;
		}

		@Override
		public String splitId() {
			return Integer.toString(id);
		}
	}

	/**
	 * A testing split reader that reads from a given list of integer arrays, where each array represents a split.
	 * The returned int array is [split, index, value].
	 */
	private static class TestingSplitReader implements SplitReader<int[], IdAndIndex> {
		private final List<int[]> records;
		private final int[] positions;
		private volatile boolean wakenUp;
		private long epoch = -1;

		TestingSplitReader(List<int[]> records) {
			this.records = records;
			this.positions = new int[records.size()];
			Arrays.fill(this.positions, -1);
			wakenUp = false;
		}

		@Override
		public void fetch(BlockingQueue<int[]> queue,
			SplitsChangesWithEpoch<IdAndIndex> splitsChangesWithEpoch,
			FinishedSplitReporter finishedSplitReporter) throws InterruptedException {
			if (epoch < splitsChangesWithEpoch.currentEpoch()) {
				Map<Long, SplitsChange<IdAndIndex>> newSplitsChanges = splitsChangesWithEpoch.splits().tailMap(epoch + 1);
				for (SplitsChange<IdAndIndex> splitsChange : newSplitsChanges.values()) {
					// split.id indicates the integer array, and split.idx indicates the position.
					splitsChange.splits().forEach(split -> positions[split.id] = split.idx);
				}
				epoch = splitsChangesWithEpoch.currentEpoch();
			}

			for (int i = 0; i < positions.length; i++) {
				int[] split = records.get(i);
				if (positions[i] >= 0 && positions[i] < split.length) {
					// [split, index, value]
					queue.put(new int[]{i, positions[i], split[positions[i]]});
					// increment position of the split.
					positions[i]++;
					// return on each element put into the queue.
					if (positions[i] == split.length) {
						finishedSplitReporter.reportFinishedSplit(Integer.toString(i));
					}
					return;
				}
			}
			synchronized (this) {
				if (!wakenUp) {
					this.wait();
				}
				wakenUp = false;
			}
		}

		@Override
		public void wakeUp() {
			synchronized (this) {
				wakenUp = true;
				this.notify();
			}
		}
	}

	/**
	 * A testing SourceReader class;
	 */
	private static final class TestingSourceReader
			extends SingleThreadMultiplexSourceReaderBase<int[], Integer, IdAndIndex> {
		private final Map<Integer, Integer> tempState;
		private volatile int[] last = new int[]{-1, -1, -1};

		TestingSourceReader(List<int[]> records) {
			super(() -> new TestingSplitReader(records));
			this.tempState = new HashMap<>();
		}

		@Override
		protected void onSplitFinished(String finishedSplitIds) {

		}

		@Override
		protected void initializedState(IdAndIndex split) {
			tempState.put(split.id, split.idx);
		}

		@Override
		protected void updateState(int[] element) {
			tempState.put(element[0], element[1]);
		}

		@Override
		protected Integer convertToEmit(int[] element) {
			if (Arrays.equals(last, element)) {
				System.out.println("Dup :: " + Arrays.toString(element));
			}
			last = element;
			return element[2];
		}

		@Override
		public List<IdAndIndex> snapshotState() {
			List<IdAndIndex> states = new ArrayList<>();
			tempState.forEach((id, idx) -> states.add(new IdAndIndex(id, idx)));
			return states;
		}
	}

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
