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

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.fetcher.SplitFetcher;
import org.apache.flink.impl.connector.source.fetcher.SplitFetcherManager;

import java.util.List;
import java.util.function.Supplier;

public abstract class SingleThreadMultiplexSourceReaderBase<E extends WithSplitId, T, SplitT extends SourceSplit, SplitStateT>
	extends SourceReaderBase<E, T, SplitT, SplitStateT> {

	public SingleThreadMultiplexSourceReaderBase(
		Supplier<SplitReader<E, SplitT>> splitFetcherSupplier,
		RecordEmitter<E, T, SplitStateT> recordEmitter) {
		super(new SingleThreadFetcherManager<>(splitFetcherSupplier), recordEmitter);
	}

	@Override
	public void configure(Configuration config) {
		super.configure(config);
		splitFetcherManager.configure(config);
		recordEmitter.configure(config);
	}

	/**
	 * A Fetcher manager with a single fetcher and assign all the splits to it.
	 */
	private static class SingleThreadFetcherManager<E extends WithSplitId, SplitT extends SourceSplit>
		extends SplitFetcherManager<E, SplitT> {

		public SingleThreadFetcherManager(Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
			super(splitReaderSupplier);
		}

		@Override
		public void addSplits(List<SplitT> splitsToAdd) {
			SplitFetcher<E, SplitT> fetcher = fetchers.get(0);
			if (fetcher == null) {
				fetcher = createSplitFetcher();
				// Add the splits to the fetchers.
				fetcher.addSplits(splitsToAdd);
				startFetcher(fetcher);
			} else {
				fetcher.addSplits(splitsToAdd);
			}
		}
	}
}
