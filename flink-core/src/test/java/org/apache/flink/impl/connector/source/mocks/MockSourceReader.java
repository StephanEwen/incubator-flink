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

import org.apache.flink.impl.connector.source.RecordsWithSplitIds;
import org.apache.flink.impl.connector.source.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.impl.connector.source.synchronization.FutureNotifier;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A mock SourceReader class;
 */
public class MockSourceReader
		extends SingleThreadMultiplexSourceReaderBase<int[], Integer, MockSplit, AtomicInteger> {


	public MockSourceReader(FutureNotifier futureNotifier,
							   FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue,
							   Supplier<SplitReader<int[], MockSplit>> splitFetcherSupplier) {
		super(futureNotifier, elementsQueue, splitFetcherSupplier, new MockRecordEmitter());
	}

	@Override
	protected void onSplitFinished(Collection<String> finishedSplitIds) {

	}

	@Override
	protected AtomicInteger initializedState(MockSplit split) {
		return new AtomicInteger(split.index());
	}

	@Override
	protected MockSplit toSplitType(String splitId, AtomicInteger splitState) {
		return new MockSplit(Integer.parseInt(splitId), splitState.get());
	}
}
