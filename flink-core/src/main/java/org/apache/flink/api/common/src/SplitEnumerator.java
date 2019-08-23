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

package org.apache.flink.api.common.src;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.src.util.ReaderLocation;

import java.util.List;
import java.util.Optional;

/**
 * The SplitEnumerator is the "work discovery" part of the source. It discovers and assigns
 * the units of work called "splits", which typically represent files (or regions of files),
 * log partitions, message queue shards, etc.
 *
 * <p>Checkpoints store a snapshot of the SplitEnumerator's state, created via
 * {@link #snapshotState()} and restored via {@link Source#restoreEnumerator(Boundedness, Object)}.
 *
 * <p>Unbounded sources that need to continuously check for new splits should implement the
 * extended interface {@link PeriodicSplitEnumerator}.
 *
 * @param <SplitT> The type of the split created by this SplitEnumerator.
 * @param <CheckpointT>
 */
@Public
public interface SplitEnumerator<SplitT, CheckpointT> extends AutoCloseable {

	/**
	 * Returns true when the input is bounded and no more splits are available.
	 * False means that the definite end of input has been reached, and is only possible
	 * in bounded sources.
	 */
	boolean isEndOfInput();

	/**
	 * Returns the next split, if it is available. If nothing is currently available, this returns
	 * an empty Optional.
	 * More may be available later, if the {@link #isEndOfInput()} is false.
	 */
	Optional<SplitT> nextSplit(ReaderLocation reader);

	/**
	 * Adds splits back to the enumerator. This happens when a reader failed and restarted,
	 * and the splits assigned to that reader since the last checkpoint need to be made
	 * available again.
	 */
	void addSplitsBack(List<SplitT> splits);

	/**
	 * Checkpoints the state of this split enumerator.
	 */
	CheckpointT snapshotState();

	/**
	 * Called to close the enumerator, in case it holds on to any resources, like threads or
	 * network connections.
	 */
	@Override
	void close() throws Exception;
}
