/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.src.lib;

import org.apache.flink.api.common.src.ReaderStatus;
import org.apache.flink.api.common.src.SourceContext;
import org.apache.flink.api.common.src.SourceOutput;
import org.apache.flink.api.common.src.SourceReader;
import org.apache.flink.api.common.src.SourceSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SourceReader} that synchronously emits data from its splits, one split after another.
 *
 * <p>This reader is for cases where the source has bounded splits (splits have a finite number of elements)
 * and the reader never needs to wait for the data, i.e., there is no waiting or blocking in any I/O operations.
 *
 * @param <E> The type of elements emitted.
 * @param <SplitT> The type of the split and state.
 */
public abstract class SynchronousSequentialReader<E, SplitT extends SourceSplit> implements SourceReader<E, SplitT> {

	private static final CompletableFuture<?> ALWAYS_COMPLETE = CompletableFuture.completedFuture(null);

	private final SourceContext sourceContext;

	private final ArrayDeque<SplitT> pendingSplits;

	@Nullable
	private SplitT currentSplit;

	private final int numSplitRequestToPipeline;

	private int numSplitRequestsPending;

	/**
	 * Creates a SynchronousSequentialReader with empty initial state.
	 */
	protected SynchronousSequentialReader(SourceContext sourceContext) {
		this(sourceContext, 1);
	}

	/**
	 * Creates a SynchronousSequentialReader with empty initial state.
	 */
	protected SynchronousSequentialReader(SourceContext sourceContext, int numSplitRequestToPipeline) {
		checkArgument(numSplitRequestToPipeline > 0, "numSplitRequestToPipeline must be positive");

		this.sourceContext = checkNotNull(sourceContext, "sourceContext");
		this.pendingSplits = new ArrayDeque<>();
		this.numSplitRequestToPipeline = numSplitRequestToPipeline;
	}

	// ------------------------------------------------------------------------
	//  source reader methods
	// ------------------------------------------------------------------------

	/**
	 *
	 */
	protected abstract ReaderStatus emitNext(SplitT currentSplit, SourceOutput<E> output) throws IOException;

	// ------------------------------------------------------------------------
	//  source reader methods
	// ------------------------------------------------------------------------

	@Override
	public void start() {
		requestSplitsIfNeeded();
	}

	@Override
	public final CompletableFuture<?> available() throws IOException {
		return ALWAYS_COMPLETE;
	}

	@Override
	public final ReaderStatus emitNext(SourceOutput<E> output) throws IOException {
		if (currentSplit != null && emitNext(currentSplit, output) != ReaderStatus.END_OF_SPLIT_DATA) {
			return ReaderStatus.MORE_AVAILABLE;
		}

		return moveToNextSplit();
	}

	private ReaderStatus moveToNextSplit() throws IOException {
		currentSplit = pendingSplits.pollFirst();
		requestSplitsIfNeeded();

		return currentSplit == null ? ReaderStatus.END_OF_SPLIT_DATA : ReaderStatus.MORE_AVAILABLE;
	}

	@Override
	public final void addSplits(List<SplitT> splits) throws IOException {
		pendingSplits.addAll(splits);
		numSplitRequestsPending = Math.max(0, numSplitRequestsPending - splits.size());
	}

	@Override
	public final List<SplitT> snapshotState() {
		final ArrayList<SplitT> splits = new ArrayList<>(numSplitsAvailable());

		if (currentSplit != null) {
			splits.add(currentSplit);
		}
		splits.addAll(pendingSplits);

		return splits;
	}

	@Override
	public void close() throws IOException {
		// nothing to do
	}

	private int numSplitsAvailable() {
		return pendingSplits.size() + (currentSplit == null ? 0 : 1);
	}

	private void requestSplitsIfNeeded() {
		int toRequest = numSplitRequestToPipeline - numSplitsAvailable() - numSplitRequestsPending;
		if (toRequest > 0) {
			sourceContext.requestNextSplits(toRequest);
		}
	}
}
