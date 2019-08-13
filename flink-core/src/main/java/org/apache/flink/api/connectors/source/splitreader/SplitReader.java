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

package org.apache.flink.api.connectors.source.splitreader;

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.impl.connector.source.FinishedSplitReporter;

import java.util.concurrent.BlockingQueue;

/**
 * An interface used to read from splits. The implementation could either read from a single split or from
 * multiple splits.
 *
 * @param <E> the element type.
 * @param <SplitT> the split type.
 */
public interface SplitReader<E, SplitT extends SourceSplit> {

	/**
	 * Fetch elements into the blocking queue for the given splits. The fetch call could be blocking
	 * but it should get unblocked when {@link #wakeUp()} is invoked. In that case, the implementation
	 * may either decide to return without throwing an exception, or it can just throw an interrupted
	 * exception. In either case, this method should be reentrant, meaning that the next fetch call
	 * should just resume from where the last fetch call was waken up or interrupted.
	 *
	 * @param queue The element queue to put the fetched element into.
	 * @param splitsChangesWithEpoch the currently assigned splits and the assignment epoch.
	 * @param finishedSplitReporter a collector to report finished splits.
	 * @throws InterruptedException when interrupted
	 */
	void fetch(
		BlockingQueue<E> queue,
		SplitsChangesWithEpoch<SplitT> splitsChangesWithEpoch,
		FinishedSplitReporter finishedSplitReporter) throws InterruptedException;

	/**
	 * Wake up the split reader in case the fetcher thread is blocking in
	 * {@link #fetch(BlockingQueue, SplitsChangesWithEpoch, FinishedSplitReporter)}.
	 */
	void wakeUp();
}
