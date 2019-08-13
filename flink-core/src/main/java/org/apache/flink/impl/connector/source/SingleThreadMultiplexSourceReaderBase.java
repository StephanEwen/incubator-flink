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
import org.apache.flink.api.connectors.source.splitreader.SplitReader;

import java.util.List;
import java.util.function.Supplier;

public abstract class SingleThreadMultiplexSourceReaderBase<E, T, SplitT extends SourceSplit>
	extends SourceReaderBase<E, T, SplitT> {

	public SingleThreadMultiplexSourceReaderBase(Supplier<SplitReader<E, SplitT>> splitFetcherSupplier) {
		super(new SingleThreadFetcherManager<>(splitFetcherSupplier));
	}

	/**
	 * A Fetcher manager with a single fetcher and assign all the splits to it.
	 */
	private static class SingleThreadFetcherManager<E, SplitT extends SourceSplit> extends SplitFetcherManager<E, SplitT> {

		public SingleThreadFetcherManager(Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
			super(splitReaderSupplier);
		}

		@Override
		protected void addSplits(List<SplitT> splitsToAdd) {
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
