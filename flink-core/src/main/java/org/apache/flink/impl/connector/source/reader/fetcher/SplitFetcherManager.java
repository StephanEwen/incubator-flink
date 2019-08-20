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

package org.apache.flink.impl.connector.source.reader.fetcher;

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.reader.Configurable;
import org.apache.flink.impl.connector.source.reader.RecordsWithSplitIds;
import org.apache.flink.impl.connector.source.reader.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.reader.SourceReaderBase;
import org.apache.flink.impl.connector.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.impl.connector.source.reader.synchronization.FutureNotifier;
import org.apache.flink.util.ThrowableCatchingRunnableWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A class responsible for starting the {@link SplitFetcher} and manage the life cycles of them.
 * This class works with the {@link SourceReaderBase}.
 */
public abstract class SplitFetcherManager<E, SplitT extends SourceSplit> implements Configurable {
	private static final Logger LOG = LoggerFactory.getLogger(SplitFetcherManager.class);

	private final ThrowableCatchingRunnableWrapper runnableWrapper;

	/** An atomic integer to generate monotonically increasing fetcher ids. */
	private final AtomicInteger fetcherIdGenerator;

	/** A supplier to provide split readers */
	private final Supplier<SplitReader<E, SplitT>> splitReaderFactory;

	/** Uncaught exception in the split fetchers.*/
	private final AtomicReference<Throwable> uncaughtFetcherException;

	/** The element queue that the split fetchers will put elements into. */
	private final BlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

	/** A map keeping track of all the split fetchers. */
	protected final Map<Integer, SplitFetcher<E, SplitT>> fetchers;

	/** An executor service with two threads. One for the fetcher and one for the future completing thread. */
	private ExecutorService executors;

	/** The configurations of this SplitFetcherManager and the SplitReaders. */
	private Configuration config;

	/**
	 * Create a split fetcher manager.
	 *
	 * @param futureNotifier a notifier to notify the complete of a future.
	 * @param elementsQueue the queue that split readers will put elements into.
	 * @param splitReaderFactory a supplier that could be used to create split readers.
	 */
	public SplitFetcherManager(FutureNotifier futureNotifier,
							   FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
							   Supplier<SplitReader<E, SplitT>> splitReaderFactory) {
		this.elementsQueue = elementsQueue;
		this.runnableWrapper = new ThrowableCatchingRunnableWrapper(new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				if (!uncaughtFetcherException.compareAndSet(null, t)) {
					// Add the exception to the exception list.
					uncaughtFetcherException.get().addSuppressed(t);
					// Wake up the main thread to let it know the exception.
					futureNotifier.notifyComplete();
				}
			}
		}, LoggerFactory.getLogger(SplitFetcher.class));
		this.splitReaderFactory = splitReaderFactory;
		this.uncaughtFetcherException = new AtomicReference<>(null);
		this.fetcherIdGenerator = new AtomicInteger(0);
		this.fetchers = new HashMap<>();

		// Create the executor with a thread factory that fails the source reader if one of
		// the fetcher thread exits abnormally.
		this.executors = Executors.newCachedThreadPool(r -> new Thread(r, "SourceFetcher"));
	}

	@Override
	public void configure(Configuration config) {
		this.config = config;
	}

	public abstract void addSplits(List<SplitT> splitsToAdd);

	protected void startFetcher(SplitFetcher<E, SplitT> fetcher) {
		executors.submit(runnableWrapper.wrap(fetcher));
	}

	protected SplitFetcher<E, SplitT> createSplitFetcher() {
		// Create SplitReader.
		SplitReader<E, SplitT> splitReader = splitReaderFactory.get();
		splitReader.configure(config);

		int fetcherId = fetcherIdGenerator.getAndIncrement();
		SplitFetcher<E, SplitT> splitFetcher = new SplitFetcher<>(
			fetcherId,
			elementsQueue,
			splitReader,
			() -> fetchers.remove(fetcherId));
		fetchers.put(fetcherId, splitFetcher);
		return splitFetcher;
	}

	public void close(long timeoutMs) throws Exception {
		fetchers.values().forEach(SplitFetcher::shutdown);
		executors.shutdown();
		if (!executors.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
			LOG.warn("Failed to close the source reader in {} ms. There are still {} split fetchers running",
				timeoutMs, fetchers.size());
		}
	}

	public void checkErrors() {
		if (uncaughtFetcherException.get() != null) {
			throw new RuntimeException("One or more fetchers have encountered exception",
				uncaughtFetcherException.get());
		}
	}
}
