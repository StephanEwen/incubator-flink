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

import org.apache.flink.api.connectors.source.SplitEnumerator;
import org.apache.flink.api.connectors.source.SourceOutput;
import org.apache.flink.api.connectors.source.SourceReader;
import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.splitreader.SplitReader;
import org.apache.flink.api.connectors.source.TimestampExtractor;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract implementation of {@link SourceReader} which provides some sychronization between
 * the mail box main thread and the SourceReader internal threads. This class allows user to
 * just provide a {@link SplitReader} and snapshot the split state.
 *
 * @param <E> The rich element type that contains information for split state update or timestamp extraction.
 * @param <T> The final element type to emit.
 * @param <SplitT> the split type.
 */
public abstract class SourceReaderBase<E, T, SplitT extends SourceSplit>
	implements SourceReader<T, SplitT>, Configurable {
	private static final Logger LOG = LoggerFactory.getLogger(SourceReaderBase.class);

	/** A queue to buffer the elements fetched by the fetcher thread. */
	private final LinkedBlockingQueue<Object> elementsQueue;

	/** The future to complete when the element queue becomes non-empty. */
	private final AtomicReference<CompletableFuture<Object>> futureRef;

	/** The split fetcher manager to run split fetchers. */
	private final SplitFetcherManager<E, SplitT> splitFetcherManager;

	/** The configuration for the reader. */
	protected SourceReaderOptions options;

	/** The raw configurations that may be used by subclasses. */
	protected Configuration configuration;

	/** The timestamp extractor to assign timestamps and generate watermarks. */
	private TimestampExtractor timestampExtractor;

	public SourceReaderBase(SplitFetcherManager<E, SplitT> splitFetcherManager) {
		this.futureRef = new AtomicReference<>(null);
		this.elementsQueue = new FutureCompletingBlockingQueue<>(futureRef);
		this.splitFetcherManager = splitFetcherManager;
		this.splitFetcherManager.setSourceReader(this);
	}

	@Override
	public void configure(Configuration config) {
		this.options = new SourceReaderOptions(config);
	}

	@Override
	public void start() {
		// Do nothing yet because we do not have any assigned splits.
	}

	@Override
	@SuppressWarnings("unchecked")
	public Status pollNext(SourceOutput<T> sourceOutput) {
		splitFetcherManager.checkErrors();
		// poll from the queue.
		Object next = elementsQueue.poll();

		Status status;
		if (next == null) {
			// No element available, set to available later if needed.
			status = Status.AVAILABLE_LATER;
		} else if (next instanceof SplitFinishedMarker) {
			// Handle the finished splits.
			onSplitFinished(((SplitFinishedMarker) next).splitId());
			// Prepare the return status based on the availability of the next element.
			status = elementsQueue.isEmpty() ? Status.AVAILABLE_LATER : Status.AVAILABLE_NOW;
		} else {
			E element = (E) next;
			// Update the state if needed.
			updateState(element);
			// Put the element into the queue
			sourceOutput.collect(convertToEmit(element));
			// Prepare the return status based on the availability of the next element.
			status = elementsQueue.isEmpty() ? Status.AVAILABLE_LATER : Status.AVAILABLE_NOW;
		}
		return status;
	}

	@Override
	public CompletableFuture<?> available() {
		splitFetcherManager.checkErrors();
		CompletableFuture<Object> future = new CompletableFuture<>();
		// The order matters here. We first set the future ref. If the element queue is empty after
		// this point, we can ensure that the future will be invoked by the fetcher once it
		// put an element into the element queue.
		this.futureRef.set(future);

		if (!elementsQueue.isEmpty()) {
			// The fetcher got the new elements after the last poll, or their is a finished split.
			// Simply complete the future and return;
			maybeCompleteFuture(futureRef);
		}
		return future;
	}

	@Override
	public void close() throws Exception {
		splitFetcherManager.close(options.sourceReaderCloseTimeout);
	}

	// -------------------- Abstract method to allow different implementations ------------------
	/**
	 * Handles the finished splits to clean the state if needed.
	 */
	protected abstract void onSplitFinished(String finishedSplitIds);

	/**
	 * When new splits are added to the reader. The initialize the state of the new splits.
	 *
	 * @param split a newly added split.
	 */
	protected abstract void initializedState(SplitT split);

	/**
	 * When an element is polled out, update the state.
	 *
	 * @param element the element that is polled out of the reader.
	 */
	protected abstract void updateState(E element);

	/**
	 * Convert an element from its rich type to the final type for emission.
	 *
	 * @param element the element to be converted then emitted.
	 * @return A convereted element to emit.
	 */
	protected abstract T convertToEmit(E element);

	/**
	 * In this abstract implementation, the expectation is that that state are continuously maintained
	 * with {@link #initializedState(SourceSplit)} and {@link #updateState(Object)} by the calling thread.
	 * So when snapshotState() is invoked, no synchronization is needed between the calling thread and
	 * fetchers.
	 */
	@Override
	public abstract List<SplitT> snapshotState();

	/**
	 * Default operation is do nothing.
	 *
	 * @param sourceEvent the event sent by the {@link SplitEnumerator}.
	 */
	@Override
	public void handleOperatorEvents(SourceEvent sourceEvent) {
		// Do nothing.
	}

	@Override
	public void addSplits(List<SplitT> splits) {
		// Initialize the state for each split.
		splits.forEach(this::initializedState);
		// Hand over the splits to the split fetcher to start fetch.
		splitFetcherManager.addSplits(splits);
	}
// ------------------ package private methods used by SplitFetcherManager -----------------

	/**
	 * Get the elements queue.
	 * @return the elements queue.
	 */
	BlockingQueue<Object> elementsQueue() {
		return elementsQueue;
	}

	/**
	 * Wakeup this main thread interacting with this source reader.
	 */
	void wakeup() {
		maybeCompleteFuture(futureRef);
	}

	// ------------------- Private methods ------------------

	/**
	 * Complete the future if there is one. This will release the thread that is waiting for data.
	 */
	private static void maybeCompleteFuture(AtomicReference<CompletableFuture<Object>> futureRef) {
		CompletableFuture<Object> future = futureRef.get();
		// If there are multiple threads trying to complete the future, only the first one succeeds.
		if (future != null && future.complete(new Object())) {
			futureRef.set(null);
		}
	}

	// ------------------- private classes -----------------

	/**
	 * A subclass of {@link LinkedBlockingQueue} that ensures all the methods adding elements into
	 * the queue will complete the elements availability future.
	 *
	 * <p>The overriding methods must first put the elements into the queue then check and complete
	 * the future if needed. This is required to ensure the thread waiting for more messages will
	 * not lose a notification.
	 *
	 * @param <T> the type of the elements in the queue.
	 */
	private static class FutureCompletingBlockingQueue<T> extends LinkedBlockingQueue<T> {
		private final AtomicReference<CompletableFuture<Object>> futureRef;

		FutureCompletingBlockingQueue(AtomicReference<CompletableFuture<Object>> futureRef) {
			this.futureRef = futureRef;
		}

		@Override
		public void put(T t) throws InterruptedException {
			super.put(t);
			maybeCompleteFuture(futureRef);
		}

		@Override
		public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
			if (super.offer(t, timeout, unit)) {
				maybeCompleteFuture(futureRef);
				return true;
			} else {
				return false;
			}
		}

		@Override
		public boolean offer(T t) {
			if (super.offer(t)) {
				maybeCompleteFuture(futureRef);
				return true;
			} else {
				return false;
			}
		}

		@Override
		public boolean add(T t) {
			if (super.add(t)) {
				maybeCompleteFuture(futureRef);
				return true;
			} else {
				return false;
			}
		}

		@Override
		public boolean addAll(Collection<? extends T> c) {
			if (super.addAll(c)) {
				maybeCompleteFuture(futureRef);
				return true;
			} else {
				return false;
			}
		}
	}
}
