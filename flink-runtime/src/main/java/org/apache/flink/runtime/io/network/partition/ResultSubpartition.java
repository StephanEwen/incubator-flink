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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.SynchronizedWriteBuffer;

import java.io.IOException;

/**
 * A single subpartition of a {@link ResultPartition} instance.
 */
public abstract class ResultSubpartition {

	/** The index of the subpartition at the parent partition. */
	protected final int index;

	/** The parent partition this subpartition belongs to. */
	protected final ResultPartition parent;

	// - Statistics ----------------------------------------------------------

	/** The total number of buffers (both data and event buffers) */
	private long totalNumberOfBuffers;

	/** The total number of bytes (both data and event buffers) */
	private long totalNumberOfBytes;

	public ResultSubpartition(int index, ResultPartition parent) {
		this.index = index;
		this.parent = parent;
	}

	protected void updateStatistics(int numberOfNewBuffers, int numberOfNewBytes) {
		totalNumberOfBuffers += numberOfNewBuffers;
		totalNumberOfBytes += numberOfNewBytes;
	}

	protected long getTotalNumberOfBuffers() {
		return totalNumberOfBuffers;
	}

	protected long getTotalNumberOfBytes() {
		return totalNumberOfBytes;
	}

	/**
	 * Notifies the parent partition about a consumed {@link ResultSubpartitionView}.
	 */
	protected void onConsumedSubpartition() {
		parent.onConsumedSubpartition(index);
	}

	protected Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	/**
	 * Adds a buffer to the subpartition.
	 *
	 * @param buffer
	 * 		the buffer to add (it is not an error to add the same buffer multiple times but the
	 * 		contents will only be sent once - use {@link org.apache.flink.runtime.io.network.buffer.Buffer#duplicate()}
	 * 		to send the same contents multiple times!)
	 * @param bytesWritten
	 * 		number of bytes added by this buffer (to keep byte counting consistent for cases where a
	 * 		single buffer is entered multiple times)
	 */
	public abstract boolean add(SynchronizedWriteBuffer buffer, int bytesWritten) throws IOException;

	public abstract void finish() throws IOException;

	public abstract void release() throws IOException;

	public abstract ResultSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException;

	abstract int releaseMemory() throws IOException;

	public abstract boolean isReleased();

	/**
	 * Makes a best effort to get the current size of the queue.
	 * This method must not acquire locks or interfere with the task and network threads in
	 * any way.
	 */
	public abstract int unsynchronizedGetNumberOfQueuedBuffers();

}
