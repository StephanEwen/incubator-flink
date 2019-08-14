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
import org.apache.flink.impl.connector.source.WithSplitId;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * The internal fetcher runnable responsible for polling message from the external system.
 */
public class SplitFetcher<E extends WithSplitId, SplitT extends SourceSplit> implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(SplitFetcher.class);
	private final int id;
	private final BlockingDeque<SplitFetcherTask> taskQueue;
	/** The current split assignments for this fetcher. */
	private final SplitsAssignments<SplitT> splitsAssignments;
	private final BlockingQueue<E> elementsQueue;
	private final SplitReader<E, SplitT> splitReader;
	private final Runnable shutdownHook;
	private final FinishedSplitReporter finishedSplitReporter;
	private final FetchTask fetchTask;
	private volatile boolean wakenUp;
	private volatile boolean closed;
	private volatile SplitFetcherTask runningTask = null;

	SplitFetcher(
		int id,
		BlockingQueue<E> elementsQueue,
		SplitReader<E, SplitT> splitReader,
		FinishedSplitReporter finishedSplitReporter,
		Runnable shutdownHook) {

		this.id = id;
		this.taskQueue = new LinkedBlockingDeque<>();
		this.elementsQueue = elementsQueue;
		this.splitsAssignments = new SplitsAssignments<>();
		this.splitReader = splitReader;
		this.shutdownHook = shutdownHook;
		this.finishedSplitReporter = finishedSplitReporter;
		this.fetchTask = new FetchTask();
		// TODO: chain an action to handle split removal.
		this.wakenUp = false;
		this.closed = false;
	}

	/**
	 * @return the current split assignments for this fetcher.
	 */
	public SplitsAssignments<SplitT> splitsAssignments() {
		return this.splitsAssignments;
	}

	@Override
	public void run() {
		try {
			while (!closed) {
				try {
					// Block at most 1 second. We don't want to naively throw InterruptedException.
					runningTask = taskQueue.peek() == null ? fetchTask : taskQueue.take();
					if (runningTask != null && runningTask.run()) {
						LOG.debug("Finished running task {}", runningTask.getClass().getSimpleName());
						// the task has finished running. Set it to null so it won't be enqueued.
						runningTask = null;
					}
				} catch (InterruptedException ie) {
					if (closed) {
						// Normal close, just return;
						return;
					} else if (wakenUp) {
						LOG.debug("Split fetcher has been waken up.");
						// The fetcher thread has just been waken up. So ignore the interrupted exception
						// and continue;
						wakenUp = false;
					} else {
						throw new RuntimeException(String.format(
							"SplitFetcher thread %d interrupted while polling the records", id), ie);
					}
				}
				// If the task is not null that means this task needs to be re-executed. This only
				// happens when the task is the fetching task or the task was interrupted.
				maybeEnqueueTask(runningTask);
			}
		} finally {
			shutdownHook.run();
		}
	}

	public void addSplits(List<SplitT> splitsToAdd) {
		maybeEnqueueTask(new SplitFetcherTask() {
			@Override
			public boolean run() throws InterruptedException {
				splitsAssignments.addSplits(splitsToAdd);
				if (!fetchTask.isRunnable()) {
					maybeEnqueueTask(fetchTask);
				}
				return true;
			}

			@Override
			public void wakeUp() {
				// No op.
			}
		});
		maybeWakeUpRunningThread();
	}

	public void shutdown() {
		LOG.info("Closing split fetcher {}", id);
		closed = true;
		SplitFetcherTask task = runningTask;
		if (task != null) {
			task.wakeUp();
		}
	}

	private void maybeWakeUpRunningThread() {
		SplitFetcherTask currentTask = runningTask;
		if (currentTask != null) {
			wakenUp = true;
			currentTask.wakeUp();
		}
	}

	private void maybeEnqueueTask(SplitFetcherTask task) {
		boolean enqueued = true;
		if (task == fetchTask) {
			enqueued = taskQueue.offer(task);
		} else if (task != null) {
			enqueued = taskQueue.offerFirst(task);
		}
		if (!enqueued) {
			throw new RuntimeException("The task queue is full. This is only theoretically possible when " +
									   "really bad thing happens.");
		}
	}

	/**
	 * The default fetch task.
	 */
	private class FetchTask implements SplitFetcherTask {
		private boolean isRunnable = true;
		@Override
		public boolean run() throws InterruptedException {
			if (!splitsAssignments.isEmpty()) {
				splitReader.fetch(elementsQueue, splitsAssignments.currentSplitsWithEpoch(), finishedSplitReporter);
			}
			isRunnable = !splitsAssignments.isEmpty();
			return splitsAssignments.isEmpty();
		}

		@Override
		public void wakeUp() {
			splitReader.wakeUp();
		}

		public boolean isRunnable() {
			return isRunnable;
		}
	}
}
