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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.PipelinedAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple wrapper for the partition readerQueue iterator, which increments a
 * sequence number for each returned buffer and remembers the receiver ID.
 */
class SequenceNumberingViewReader implements PipelinedAvailabilityListener {

	private final Object requestLock = new Object();

	private final InputChannelID receiverId;

	private final AtomicLong numBuffersAvailable = new AtomicLong();

	private final PartitionRequestQueue requestQueue;

	private volatile ResultSubpartitionView subpartitionView;

	private int sequenceNumber = -1;

	SequenceNumberingViewReader(InputChannelID receiverId, PartitionRequestQueue requestQueue) {
		this.receiverId = receiverId;
		this.requestQueue = requestQueue;
	}

	void requestSubpartitionView(
		ResultPartitionProvider partitionProvider,
		ResultPartitionID resultPartitionId,
		int subPartitionIndex,
		BufferProvider bufferProvider) throws IOException {

		synchronized (requestLock) {
			this.subpartitionView = partitionProvider.createSubpartitionView(
				resultPartitionId,
				subPartitionIndex,
				bufferProvider,
				this);
		}
	}

	InputChannelID getReceiverId() {
		return receiverId;
	}

	int getSequenceNumber() {
		return sequenceNumber;
	}

	public BufferAndAvailability getNextBuffer() throws IOException, InterruptedException {
		ResultSubpartitionView subpartitionView = this.subpartitionView;
		if (subpartitionView == null) {
			// this can happen if the request for the partition was triggered asynchronously
			// by the time trigger
			// would be good to avoid that, by guaranteeing that the requestPartition() and
			// getNextBuffer() always come from the same thread
			// we could do that by letting the timer insert a special "requesting channel" into the input gate's queue
			subpartitionView = checkAndWaitForSubpartitionView();
		}

		Buffer next = subpartitionView.getNextBuffer();
		if (next != null) {
			long remaining = numBuffersAvailable.decrementAndGet();
			sequenceNumber++;

			if (remaining >= 0) {
				return new BufferAndAvailability(next, remaining > 0);
			} else {
				throw new IllegalStateException("no buffer available");
			}
		} else {
			return null;
		}
	}

	public void notifySubpartitionConsumed() throws IOException {
		subpartitionView.notifySubpartitionConsumed();
	}

	public boolean isReleased() {
		return subpartitionView.isReleased();
	}

	public Throwable getFailureCause() {
		return subpartitionView.getFailureCause();
	}

	public void releaseAllResources() throws IOException {
		subpartitionView.releaseAllResources();
	}

	private ResultSubpartitionView checkAndWaitForSubpartitionView() {
		// synchronizing on the request lock means this blocks until the asynchronous request
		// for the partition view has been completed
		// by then the subpartition view is visible or the channel is released
		synchronized (requestLock) {
			return subpartitionView;
		}
	}

	@Override
	public void notifyBuffersAvailable(long numBuffers) {
		// if this request made the channel non-empty, notify the input gate
		if (numBuffers > 0 && numBuffersAvailable.getAndAdd(numBuffers) == 0) {
			requestQueue.notifyReaderNonEmpty(this);
		}
	}
}
