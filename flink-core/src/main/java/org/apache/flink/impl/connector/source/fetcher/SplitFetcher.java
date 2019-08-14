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
import org.apache.flink.impl.connector.source.splitreader.SplitsAddition;
import org.apache.flink.impl.connector.source.splitreader.SplitsChange;
import org.apache.flink.impl.connector.source.splitreader.SplitsRemoval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
	// track the assigned splits so we can suspend the reader when there is no splits assigned.
	private final Map<String, SplitT> assignedSplits;
	/** The current split assignments for this fetcher. */
	private final Queue<SplitsChange<SplitT>> splitChanges;
	private final BlockingQueue<E> elementsQueue;
	private final SplitReader<E, SplitT> splitReader;
	private final Runnable shutdownHook;
	private final SplitFinishedCallback splitFinishedCallback;
	private final FetchTask fetchTask;
	private volatile boolean wakenUp;
	private volatile boolean closed;
	private volatile SplitFetcherTask runningTask = null;

	SplitFetcher(
		int id,
		BlockingQueue<E> elementsQueue,
		SplitReader<E, SplitT> splitReader,
		SplitFinishedCallback splitFinishedCallback,
		Runnable shutdownHook) {

		this.id = id;
		this.taskQueue = new LinkedBlockingDeque<>();
		this.elementsQueue = elementsQueue;
		this.splitChanges = new LinkedList<>();
		this.assignedSplits = new HashMap<>();
		this.splitReader = splitReader;
		this.shutdownHook = shutdownHook;
		this.splitFinishedCallback = splitFinishedCallback;
		// Remove the split from the assignments if it is already done.
		this.splitFinishedCallback.chainAction(assignedSplits::remove);
		this.fetchTask = new FetchTask();
		this.wakenUp = false;
		this.closed = false;
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

	/**
	 * Add splits to the split fetcher. This operation is asynchronous.
	 *
	 * @param splitsToAdd the splits to add.
	 */
	public void addSplits(List<SplitT> splitsToAdd) {
		maybeEnqueueTask(() -> {
			splitChanges.add(new SplitsAddition<>(splitsToAdd));
			splitsToAdd.forEach(s -> assignedSplits.put(s.splitId(), s));
			if (!fetchTask.isRunnable()) {
				maybeEnqueueTask(fetchTask);
			}
			return true;
		});
		maybeWakeUpRunningThread();
	}

	/**
	 * Remove splits from the fetcher. This operation is asynchronous.
	 *
	 * @param splitsToRemove the splits to remove.
	 */
	public void removeSplits(List<SplitT> splitsToRemove) {
		maybeEnqueueTask(() -> {
			splitChanges.add(new SplitsRemoval<>(splitsToRemove));
			splitsToRemove.forEach(s -> assignedSplits.remove(s.splitId()));
			if (!fetchTask.isRunnable()) {
				maybeEnqueueTask(fetchTask);
			}
			return true;
		});
		maybeWakeUpRunningThread();
	}

	/**
	 * Shutdown the split fetcher.
	 */
	public void shutdown() {
		LOG.info("Closing split fetcher {}", id);
		closed = true;
		maybeWakeUpRunningThread();
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
			if (!assignedSplits.isEmpty() || !splitChanges.isEmpty()) {
				splitReader.fetch(elementsQueue, splitChanges, splitFinishedCallback);
			}
			isRunnable = !assignedSplits.isEmpty() || !splitChanges.isEmpty();
			// If it is not runnable then the work is done.
			return !isRunnable;
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
