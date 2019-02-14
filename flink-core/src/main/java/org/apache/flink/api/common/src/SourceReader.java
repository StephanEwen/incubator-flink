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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The SourceReader reads the actual source data from the input splits.
 *
 * <p>The SourceReader handles potentially multiple split at the same time, for example when
 * one reader polls from multiple partitions or shards of a message queue at the same time.
 * If the reader reads bounded splits one after another, consider using the
 * {@link SequentialReader}.
 *
 * <p>The SourceReader is non blocking: It signals data availability through a future,
 * rather than having a blocking method to read data or advance the input.
 * Most source readers that require blocking read logic should use the
 *
 * @param <E>
 * @param <SplitT>
 */
public interface SourceReader<E, SplitT> extends Closeable {

	void start() throws IOException;

	/**
	 * Returns the future that signals availability of data.
	 *
	 * <p>The future MUST be completed when data is available or when the end-of-split has been
	 * reached.
	 *
	 * <p>The future SHOULD NOT be completed when there is more data in the current split,
	 * but it is currently not available. If the future is completed nonetheless, the
	 * {@link #emitNext(SourceOutput)} method will be called. If the future is frequently completed
	 * even though no data is available, the {@code #emitNext(SourceOutput)} will be called frequently
	 * without any data being ready to emit, making the reader inefficient and causing busy waiting
	 * loops in the worst case.
	 *
	 * <p>If a source only ever returns {@link ReaderStatus#MORE_AVAILABLE} and
	 * {@link ReaderStatus#END_OF_SPLIT_DATA}, then the future may always be completed.
	 */
	CompletableFuture<?> available() throws IOException;

	ReaderStatus emitNext(SourceOutput<E> output) throws IOException;

	void addSplits(List<SplitT> splits) throws IOException;

	List<SplitT> snapshotState();
}
