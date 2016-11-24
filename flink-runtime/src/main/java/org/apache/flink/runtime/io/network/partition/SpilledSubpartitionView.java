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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.SynchronousBufferFileReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * View over a spilled subpartition.
 *
 * <p> Reads are done synchronously.
 */
class SpilledSubpartitionView implements ResultSubpartitionView, NotificationListener {

	/** The subpartition this view belongs to. */
	private final ResultSubpartition parent;

	/** The synchronous file reader to do the actual I/O. */
	private final BufferFileReader fileReader;

	/** The buffer pool to read data into. */
	private final SpillReadBufferPool bufferPool;

	/** Buffer availability listener. */
	private final PipelinedAvailabilityListener availabilityListener;

	private final long numberOfSpilledBuffers;

	/** Flag indicating whether all resources have been released. */
	private AtomicBoolean isReleased = new AtomicBoolean();

	private volatile boolean isSpillInProgress = true;

	SpilledSubpartitionView(
			ResultSubpartition parent,
			int memorySegmentSize,
			BufferFileWriter spillWriter,
			long numberOfSpilledBuffers,
			PipelinedAvailabilityListener availabilityListener) throws IOException {

		this.parent = checkNotNull(parent);
		this.bufferPool = new SpillReadBufferPool(2, memorySegmentSize);
		this.fileReader = new SynchronousBufferFileReader(spillWriter.getChannelID(), false);
		checkArgument(numberOfSpilledBuffers >= 0);
		this.numberOfSpilledBuffers = numberOfSpilledBuffers;
		this.availabilityListener = checkNotNull(availabilityListener);

		// Check whether async spilling is still in progress. If not, this returns
		// false and we can notify our availability listener about all available buffers.
		// Otherwise, we notify only when the spill writer callback happens.
		if (!spillWriter.registerAllRequestsProcessedListener(this)) {
			isSpillInProgress = false;
			availabilityListener.notifyBuffersAvailable(numberOfSpilledBuffers);
		}
	}

	/**
	 * This is the call back method for the spill writer. If a spill is still
	 * in progress when this view is created we wait until this method is called
	 * before we notify the availability listener.
	 */
	@Override
	public void onNotification() {
		isSpillInProgress = false;
		availabilityListener.notifyBuffersAvailable(numberOfSpilledBuffers);
	}

	@Override
	public Buffer getNextBuffer() throws IOException, InterruptedException {
		if (fileReader.hasReachedEndOfFile() || isSpillInProgress) {
			return null;
		}

		// It's OK to request the buffer in a blocking fashion as the buffer pool is NOT shared
		// among all consumed subpartitions.
		// TODO This is very, very fragile.
		final Buffer buffer = bufferPool.requestBufferBlocking();

		fileReader.readInto(buffer);

		return buffer;
	}

	@Override
	public void notifyBuffersAvailable(int buffers) throws IOException {
		// We do the availability listener notification either directly on
		// construction of this view (when everything has been spilled) or
		// as soon as spilling is done and we are notified about it in the
		// callback.
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		parent.onConsumedSubpartition();
	}

	@Override
	public void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			fileReader.close();
			bufferPool.destroy();
		}
	}

	@Override
	public boolean isReleased() {
		return parent.isReleased() || isReleased.get();
	}

	@Override
	public Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	@Override
	public String toString() {
		return String.format("SpilledSubpartitionView[sync](index: %d) of ResultPartition %s", parent.index, parent.parent.getPartitionId());
	}

	/**
	 * A buffer pool to provide buffer to read the file into.
	 *
	 * <p> This pool ensures that a consuming input gate makes progress in all cases, even when all
	 * buffers of the input gate buffer pool have been requested by remote input channels.
	 *
	 * TODO Replace with asynchronous buffer pool request as this introduces extra buffers per
	 * consumed subpartition.
	 */
	private static class SpillReadBufferPool implements BufferRecycler {

		private final Queue<Buffer> buffers;

		private boolean isDestroyed;

		SpillReadBufferPool(int numberOfBuffers, int memorySegmentSize) {
			this.buffers = new ArrayDeque<Buffer>(numberOfBuffers);

			synchronized (buffers) {
				for (int i = 0; i < numberOfBuffers; i++) {
					buffers.add(new Buffer(MemorySegmentFactory.allocateUnpooledSegment(memorySegmentSize), this));
				}
			}
		}

		@Override
		public void recycle(MemorySegment memorySegment) {
			synchronized (buffers) {
				if (isDestroyed) {
					memorySegment.free();
				}
				else {
					buffers.add(new Buffer(memorySegment, this));
					buffers.notifyAll();
				}
			}
		}

		private Buffer requestBufferBlocking() throws InterruptedException {
			synchronized (buffers) {
				while (true) {
					if (isDestroyed) {
						return null;
					}

					Buffer buffer = buffers.poll();

					if (buffer != null) {
						return buffer;
					}
					// Else: wait for a buffer
					buffers.wait();
				}
			}
		}

		private void destroy() {
			synchronized (buffers) {
				isDestroyed = true;
				buffers.notifyAll();
			}
		}
	}
}
