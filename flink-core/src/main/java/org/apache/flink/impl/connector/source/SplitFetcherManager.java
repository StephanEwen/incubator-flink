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
public abstract class SplitFetcherManager<E, SplitT extends SourceSplit> {
	private static final Logger LOG = LoggerFactory.getLogger(SplitFetcherManager.class);

	/** An atomic integer to generate monotonically increasing fetcher ids. */
	private final AtomicInteger fetcherIdGenerator;

	/** A supplier to provide split readers */
	private final Supplier<SplitReader<E, SplitT>> splitReaderSupplier;

	/** Uncaught exception in the split fetchers.*/
	private final AtomicReference<Throwable> uncaughtFetcherException;

	/** A map keeping track of all the split fetchers. */
	protected final Map<Integer, SplitFetcher<E, SplitT>> fetchers;

	/** The source reader this split fetcher manager is working with. */
	private SourceReaderBase<E, ?, SplitT> sourceReader;

	/** An executor service with two threads. One for the fetcher and one for the future completing thread. */
	private ExecutorService executors;

	/** A finished split reporter to allow the split fetcher to report finished splits.
	 * This is needed in order to clean up the split states maintained in the source reader. */
	private FinishedSplitReporter finishedSplitReporter;

	/**
	 * Create a split fetcher manager.
	 */
	public SplitFetcherManager(Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
		this.splitReaderSupplier = splitReaderSupplier;
		this.uncaughtFetcherException = new AtomicReference<>(null);
		this.fetcherIdGenerator = new AtomicInteger(0);
		this.fetchers = new HashMap<>();
	}

	/**
	 * Set the source reader this SplitFetcherManager will be working with.
	 *
	 * <p>The method is package private because we do not expect users to set this explicitly.
	 *
	 * @param sourceReader the source Reader this split manager will work with.
	 */
	void setSourceReader(SourceReaderBase<E, ?, SplitT> sourceReader) {
		this.sourceReader = sourceReader;
		// Create the executor with a thread factory that fails the source reader if one of
		// the fetcher thread exits abnormally.
		this.executors = Executors.newCachedThreadPool(new SourceReaderThreadFactory((t) -> {
			if (!uncaughtFetcherException.compareAndSet(null, t)) {
				// Add the exception to the exception list.
				uncaughtFetcherException.get().addSuppressed(t);
				// Wake up the main thread to let it know the exception.
				sourceReader.wakeup();
			}
		}));
		// The finished split reporter simply enqueues a SplitFinishedMarker.
		this.finishedSplitReporter = new FinishedSplitReporter(new Consumer<String>() {
			@Override
			public void accept(String splitId) {
				try {
					sourceReader.elementsQueue().put(new SplitFinishedMarker(splitId));
				} catch (InterruptedException e) {
					throw new RuntimeException("Interrupted while reporting the finished split " + splitId);
				}
			}
		});
	}

	protected abstract void addSplits(List<SplitT> splitsToAdd);

	protected void startFetcher(SplitFetcher<E, SplitT> fetcher) {
		executors.submit(fetcher);
	}

	@SuppressWarnings("unchecked")
	protected SplitFetcher<E, SplitT> createSplitFetcher() {
		int fetcherId = fetcherIdGenerator.getAndIncrement();
		SplitFetcher<E, SplitT> splitFetcher = new SplitFetcher<>(
			fetcherId,
			(BlockingQueue<E>)  sourceReader.elementsQueue(),
			splitReaderSupplier.get(),
			finishedSplitReporter,
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
			throw new RuntimeException("One ore more fetcher has encountered exception",
				uncaughtFetcherException.get());
		}
	}
}
