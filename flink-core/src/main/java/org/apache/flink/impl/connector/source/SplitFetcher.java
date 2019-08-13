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

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * The internal fetcher runnable responsible for polling message from the external system.
 */
public class SplitFetcher<E, SplitT extends SourceSplit> implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(SplitFetcher.class);
	private final int id;
	/** The current split assignments for this fetcher. */
	private final SplitsAssignments<SplitT> splitsAssignments;
	private final BlockingQueue<E> elementsQueue;
	private final SplitReader<E, SplitT> splitReader;
	private final Runnable shutdownHook;
	private final FinishedSplitReporter finishedSplitReporter;
	private volatile boolean wakenUp;
	private volatile boolean closed;

	SplitFetcher(
		int id,
		BlockingQueue<E> elementsQueue,
		SplitReader<E, SplitT> splitReader,
		FinishedSplitReporter finishedSplitReporter,
		Runnable shutdownHook) {
		this.id = id;
		this.elementsQueue = elementsQueue;
		this.splitsAssignments = new SplitsAssignments<>();
		this.splitReader = splitReader;
		this.shutdownHook = shutdownHook;
		this.finishedSplitReporter = finishedSplitReporter;
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
					// Block on the assignment empty. Note that this does not guarantee the splits assignment
					// passed to the splitReader.fetch() is non-empty. This blocking call simply tries to avoid the
					// case that there is no
					splitsAssignments.blockOnEmpty();
					splitReader.fetch(elementsQueue, splitsAssignments.currentSplitsWithEpoch(), finishedSplitReporter);
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
			}
		} finally {
			shutdownHook.run();
		}
	}

	public void addSplits(List<SplitT> splitsToAdd) {
		splitsAssignments.addSplits(splitsToAdd);
		splitReader.wakeUp();
	}

	public void shutdown() {
		LOG.info("Closing split fetcher {}", id);
		closed = true;
		splitReader.wakeUp();
	}
}
