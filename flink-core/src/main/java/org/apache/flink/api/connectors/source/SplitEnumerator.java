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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * The enumerator is responsible for doing the following:
 * 1. discover the splits for the {@link SourceReader} to read.
 * 2. assign the splits to the source reader.
 */
public interface SplitEnumerator<SplitT, CheckpointT> extends AutoCloseable {

	/**
	 * Indicate whether there is more unassigned splits to be read.
	 *
	 * @return true if there are more splits to read.
	 */
	boolean isEndOfInput();

	/**
	 * Assign a split to assign to the given reader.
	 *
	 * @param readerLocation the location of the reader. Assuming it is String for now.
	 * @param subtaskIndex the reader subtask index id.
	 * @return An optional Split to assign to the source reader.
	 */
	Optional<SplitT> nextSplit(String readerLocation, int subtaskIndex);

	/**
	 * Add a split back to the split enumerator. It will only happen when a {@link SourceReader} fails
	 * and there are splits assigned to it after the last successful checkpoint.
	 *
	 * @param splits the split to add back to the enumerator for reassignment.
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
	void close() throws IOException;
}
