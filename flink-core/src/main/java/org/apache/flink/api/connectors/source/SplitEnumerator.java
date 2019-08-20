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

package org.apache.flink.api.connectors.source;

import org.apache.flink.api.connectors.source.event.SourceEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The enumerator is responsible for doing the following:
 * 1. discover the splits for the {@link SourceReader} to read.
 * 2. assign the splits to the source reader.
 */
public interface SplitEnumerator<SplitT, CheckpointT> extends AutoCloseable {

	/**
	 * Handles the source event from the source reader.
	 *
	 * @param subtaskId the subtask id of the source reader who sent the source event.
	 * @param sourceEvent the source event from the source reader.
	 */
	void handleSourceEvent(int subtaskId, SourceEvent sourceEvent);

	/**
	 * Add a split back to the split enumerator. It will only happen when a {@link SourceReader} fails
	 * and there are splits assigned to it after the last successful checkpoint.
	 *
	 * @param splits the split to add back to the enumerator for reassignment.
	 */
	void addSplitsBack(List<SplitT> splits);

	/**
	 * A method that returns a future that will be completed when a split assignment is available.
	 * A typical implementation would just assign the splits synchronously when this method is
	 * invoked for the first time. And afterwards, only update the assignments when some event
	 * happens, some examples of the events are:
	 * 1. receiving a new SourceEvent from the SourceReader
	 * 2. some splits are added back to the enumerator
	 * 3. a new split is discovered (by an internal thread)
	 *
	 * <p>When a new source reader is registered, this method will be invoked again to get a new
	 * split assignment.
	 *
	 * @param registeredReader the currently registered readers.
	 * @return a future that will be completed when a new assignment is available.
	 */
	CompletableFuture<SplitsAssignment<SplitT>> assignSplits(Map<Integer, ReaderInfo> registeredReader);

	/**
	 * Checkpoints the state of this split enumerator.
	 */
	CheckpointT snapshotState();

	/**
	 * Called to close the enumerator, in case it holds on to any resources, like threads or
	 * network connections.
	 */
	@Override
	void close() throws IOException;
}
