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
import org.apache.flink.impl.connector.source.RecordsWithSplitIds;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * The default fetch task that fetches the records into the element queue.
 */
class FetchTask<E, SplitT extends SourceSplit> implements SplitFetcherTask {
	private final SplitReader<E, SplitT> splitReader;
	private final BlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
	private final Consumer<String> splitFinishedCallback;

	FetchTask(SplitReader<E, SplitT> splitReader,
			  BlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
			  Consumer<String> splitFinishedCallback) {
		this.splitReader = splitReader;
		this.elementsQueue = elementsQueue;
		this.splitFinishedCallback = splitFinishedCallback;
	}

	@Override
	public boolean run() throws InterruptedException {
		splitReader.fetch(elementsQueue, splitFinishedCallback);
		// It is important to return true here so that the running task.
		return true;
	}

	@Override
	public void wakeUp() {
		splitReader.wakeUp();
	}

	@Override
	public String toString() {
		return "FetchTask";
	}
}
