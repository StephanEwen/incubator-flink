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

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.SynchronizedWriteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A spillable sub partition starts out in-memory and spills to disk if asked
 * to do so.
 *
 * <p>Buffers for the partition come from a {@link BufferPool}. The buffer pool
 * is also responsible to trigger the release of the buffers if it needs them
 * back. At this point, the spillable sub partition will write all in-memory
 * buffers to disk. All added buffers after that point directly go to disk.
 *
 * <p>This partition type is used for {@link ResultPartitionType#BLOCKING}
 * results that are fully produced before they can be consumed. At the point
 * when they are consumed, the buffers are (i) all in-memory, (ii) currently
 * being spilled to disk, or (iii) completely spilled to disk. Depending on
 * this state, different reader variants are returned (see
 * {@link SpillableSubpartitionView} and {@link SpilledSubpartitionView}).
 *
 * <p>Since the network buffer pool size for outgoing partitions is usually
 * quite small, e.g. via the {@link TaskManagerOptions#NETWORK_BUFFERS_PER_CHANNEL}
 * and {@link TaskManagerOptions#NETWORK_EXTRA_BUFFERS_PER_GATE} parameters
 * for bounded channels or from the default values of
 * {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},
 * {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN}, and
 * {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}, most spillable partitions
 * will be spilled for real-world data sets.
 */
class SpillableSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(SpillableSubpartition.class);

	/** Buffers are kept in this queue as long as we weren't ask to release any. */
	private final ArrayDeque<SynchronizedWriteBuffer> buffers = new ArrayDeque<>();

	/** The I/O manager used for spilling buffers to disk. */
	private final IOManager ioManager;

	/** The writer used for spilling. As long as this is null, we are in-memory. */
	private BufferFileWriter spillWriter;

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	/** Flag indicating whether the subpartition has been released. */
	private volatile boolean isReleased;

	/** The read view to consume this subpartition. */
	private ResultSubpartitionView readView;

	SpillableSubpartition(int index, ResultPartition parent, IOManager ioManager) {
		super(index, parent);

		this.ioManager = checkNotNull(ioManager);
	}

	@Override
	public boolean add(SynchronizedWriteBuffer buffer, int bytesWritten) throws IOException {
		checkNotNull(buffer);

		synchronized (buffers) {
			if (isFinished || isReleased) {
				return false;
			}

			// The number of buffers are needed later when creating
			// the read views. Be sure to call updateStatistics() or
			// at least make sure to still count the number of buffers.

			if (spillWriter == null) {

				// Add the buffer (only if different to the head of the queue) and update the stats
				if (buffer != buffers.peekLast()) {
					buffers.add(buffer);
					updateStatistics(1, bytesWritten);
				} else {
					buffer.recycleBuffer(); // not using this reference
					// already notified by the previous add() operation of this buffer
					updateStatistics(0, bytesWritten);
				}

				return true;
			}
		}

		// Didn't return early => go to disk
		SpillResult spillResult = spillBuffer(buffer, true);
		updateStatistics(spillResult.spilledBuffers, bytesWritten);

		return true;
	}

	/**
	 * Spills the given buffer to disk.
	 *
	 * <p>Also takes care of defining a (fixed) area of the buffer to spill/consume similar to
	 * {@link SpillableSubpartitionView#getNextBuffer()} and {@link PipelinedSubpartition#pollBuffer()}
	 * as the buffer's contents may still be changed in parallel, e.g. by the {@link
	 * org.apache.flink.runtime.io.network.api.writer.RecordWriter} adding contents to the buffer.
	 *
	 * @param buffer
	 * 		buffer to spill
	 * @param skipEmpty
	 * 		whether to skip spilling empty buffers
	 *
	 * @return number of spilled buffers and bytes
	 *
	 * @throws IOException
	 * 		if the writer encounters an I/O error.
	 */
	private SpillResult spillBuffer(SynchronizedWriteBuffer buffer, boolean skipEmpty)
			throws IOException {
		return spillBuffer(spillWriter, buffer, skipEmpty);
	}

	/**
	 * Spills the given buffer to disk.
	 *
	 * <p>Also takes care of defining a (fixed) area of the buffer to spill/consume similar to
	 * {@link SpillableSubpartitionView#getNextBuffer()} and {@link PipelinedSubpartition#pollBuffer()}
	 * as the buffer's contents may still be changed in parallel, e.g. by the {@link
	 * org.apache.flink.runtime.io.network.api.writer.RecordWriter} adding contents to the buffer.
	 *
	 * @param spillWriter
	 * 		buffer writer to use
	 * @param buffer
	 * 		buffer to spill
	 * @param skipEmpty
	 * 		whether to skip spilling empty buffers
	 *
	 * @return number of spilled buffers and bytes
	 *
	 * @throws IOException
	 * 		if the writer encounters an I/O error.
	 */
	static SpillResult spillBuffer(
			BufferFileWriter spillWriter, SynchronizedWriteBuffer buffer, boolean skipEmpty)
			throws IOException {
		// stop the writer from continuing to write to the buffer and continue with an independent
		// duplicate here
		Buffer duplicate = buffer.sealAndDuplicate();
		// we've "consumed" all readable bytes, i.e. those are forwarded into the stack and
		// should not be forwarded again. Note that only this thread modifies the reader index when
		// spilling and we will always spill once started, also in the SpillableSubpartitionView
		// working together with this class.
		((Buffer) buffer).setReaderIndex(duplicate.getWriterIndex());

		// buffers with no remaining data have already been written but may also have been announced
		int readableBytes = duplicate.readableBytes();

		if (!skipEmpty || readableBytes > 0) {
			// duplicate (and thus also the original) will be recycled after the write operation
			spillWriter.writeBlock(duplicate);
			return new SpillResult(1, readableBytes);
		} else {
			buffer.recycleBuffer();
			return new SpillResult(0, readableBytes);
		}
	}

	@Override
	public void finish() throws IOException {
		Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);
		int bytesWritten = buffer.getWriterIndex();

		synchronized (buffers) {
			if (add(buffer, bytesWritten)) {
				isFinished = true;
			}
		}

		// If we are spilling/have spilled, wait for the writer to finish
		if (spillWriter != null) {
			spillWriter.close();
		}
	}

	@Override
	public void release() throws IOException {
		final ResultSubpartitionView view;

		synchronized (buffers) {
			if (isReleased) {
				return;
			}

			view = readView;

			// No consumer yet, we are responsible to clean everything up. If
			// one is available, the view is responsible is to clean up (see
			// below).
			if (view == null) {
				for (SynchronizedWriteBuffer buffer : buffers) {
					buffer.recycleBuffer();
				}
				buffers.clear();

				// TODO This can block until all buffers are written out to
				// disk if a spill is in-progress before deleting the file.
				// It is possibly called from the Netty event loop threads,
				// which can bring down the network.
				if (spillWriter != null) {
					spillWriter.closeAndDelete();
				}
			}

			isReleased = true;
		}

		if (view != null) {
			view.releaseAllResources();
		}
	}

	@Override
	public ResultSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		synchronized (buffers) {
			if (!isFinished) {
				throw new IllegalStateException("Subpartition has not been finished yet, " +
					"but blocking subpartitions can only be consumed after they have " +
					"been finished.");
			}

			if (readView != null) {
				throw new IllegalStateException("Subpartition is being or already has been " +
					"consumed, but we currently allow subpartitions to only be consumed once.");
			}

			if (spillWriter != null) {
				readView = new SpilledSubpartitionView(
					this,
					parent.getBufferProvider().getMemorySegmentSize(),
					spillWriter,
					getTotalNumberOfBuffers(),
					availabilityListener);
			} else {
				readView = new SpillableSubpartitionView(
					this,
					buffers,
					ioManager,
					parent.getBufferProvider().getMemorySegmentSize(),
					availabilityListener);
			}

			return readView;
		}
	}

	@Override
	public int releaseMemory() throws IOException {
		synchronized (buffers) {
			ResultSubpartitionView view = readView;

			if (view != null && view.getClass() == SpillableSubpartitionView.class) {
				// If there is a spilalble view, it's the responsibility of the
				// view to release memory.
				SpillableSubpartitionView spillableView = (SpillableSubpartitionView) view;
				return spillableView.releaseMemory();
			} else if (spillWriter == null) {
				// No view and in-memory => spill to disk
				spillWriter = ioManager.createBufferFileWriter(ioManager.createChannel());

				int numberOfBuffers = buffers.size();
				long spilledBytes = 0;

				// Spill all buffers
				for (int i = 0; i < numberOfBuffers; i++) {
					SynchronizedWriteBuffer buffer = buffers.remove();
					// note: we always need to spill buffers from the queue, even if empty, since we
					//       already announced them
					spilledBytes += spillBuffer(buffer, false).spilledBytes;
				}

				LOG.debug("Spilling {} bytes for sub partition {} of {}.", spilledBytes, index, parent.getPartitionId());

				return numberOfBuffers;
			}
		}

		// Else: We have already spilled and don't hold any buffers
		return 0;
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		// since we do not synchronize, the size may actually be lower than 0!
		return Math.max(buffers.size(), 0);
	}

	@Override
	public String toString() {
		return String.format("SpillableSubpartition [%d number of buffers (%d bytes)," +
						"finished? %s, read view? %s, spilled? %s]",
				getTotalNumberOfBuffers(), getTotalNumberOfBytes(), isFinished, readView != null,
				spillWriter != null);
	}

	// ------------------------------------------------------------------------

	/**
	 * Result of the {@link #spillBuffer(BufferFileWriter, SynchronizedWriteBuffer, boolean)} operation containing
	 * the number of spilled buffers and the spilled bytes.
	 */
	static final class SpillResult {

		public final int spilledBuffers;
		public final int spilledBytes;

		public SpillResult(int spilledBuffers, int spilledBytes) {
			this.spilledBuffers = spilledBuffers;
			this.spilledBytes = spilledBytes;
		}
	}
}
