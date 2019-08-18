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

package org.apache.flink.impl.connector.source.mocks;

import org.apache.flink.api.connectors.source.SourceSplit;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Simple testing splits.
 */
public class MockSplit implements SourceSplit {
	private final int id;
	private final BlockingQueue<Integer> records;
	private final int endIndex;
	private int index;

	public MockSplit(int id) {
		this(id, 0);
	}

	public MockSplit(int id, int startingIndex) {
		this(id, startingIndex, Integer.MAX_VALUE);
	}

	public MockSplit(int id, int startingIndex, int endIndex) {
		this.id = id;
		this.endIndex = endIndex;
		this.index = startingIndex;
		this.records = new LinkedBlockingQueue<>();
	}

	@Override
	public String splitId() {
		return Integer.toString(id);
	}

	public int index() {
		return index;
	}

	public int endIndex() {
		return endIndex;
	}

	boolean isFinished() {
		return index == endIndex;
	}

	/**
	 * Get the next element. Block if asked.
	 */
	public int[] getNext(boolean blocking) throws InterruptedException {
		Integer value = blocking ? records.take() : records.poll();
		return value == null ? null : new int[]{value, index++};
	}

	/**
	 * Add a record to this split.
	 */
	public void addRecord(int record) {
		if (!records.offer(record)) {
			throw new IllegalStateException("Failed to add record to split.");
		}
	}
}
