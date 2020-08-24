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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link ResultPartition} that turns records into buffers and write directly to partitions.
 * This is in contrast to implementations where records are written to a joint
 * structure, from which the subpartitions draw the data after the write phase is finished,
 * for example sort-based partitioning.
 *
 * <p>To avoid confusion: On the read side, all partitions return buffers (and backlog) to be
 * transported through the network ch
 */
public class BufferWritingResultPartition extends ResultPartition {

	private final BufferBuilder[] currentPartitionBuffers;

	@Nullable
	private BufferBuilder broadcastBuffer;

	private final BufferWritingSubpartition[] bufferWritingSubpartitions;

	private Counter numBuffersOut = new SimpleCounter();

	private Meter idleTimeMsPerSecond = new MeterView(new SimpleCounter());

	public BufferWritingResultPartition(
			String owningTaskName,
			int partitionIndex,
			ResultPartitionID partitionId,
			ResultPartitionType partitionType,
			BufferWritingSubpartition[] subpartitions,
			int numTargetKeyGroups,
			ResultPartitionManager partitionManager,
			@Nullable BufferCompressor bufferCompressor,
			FunctionWithException<BufferPoolOwner,
			BufferPool, IOException> bufferPoolFactory) {

		super(
			owningTaskName,
			partitionIndex,
			partitionId,
			partitionType,
			subpartitions,
			numTargetKeyGroups,
			partitionManager,
			bufferCompressor,
			bufferPoolFactory);

		this.bufferWritingSubpartitions = subpartitions;
		this.currentPartitionBuffers = new BufferBuilder[subpartitions.length];
	}

	public void emitRecord(ByteBuffer recordBytes, int targetPartition) throws IOException {
		do {
			final BufferBuilder bufferBuilder = getBufferBuilderForSinglePartitionEmit(targetPartition);
			bufferBuilder.appendAndCommit(recordBytes);

			if (bufferBuilder.isFull()) {
				finishPartitionBuffer(targetPartition);
			}
		}
		while (recordBytes.hasRemaining());
	}

	public void emitRecordToAllPartitions(ByteBuffer recordBytes) throws IOException {
		for (int targetChannel = 0; targetChannel < bufferWritingSubpartitions.length; targetChannel++) {
			recordBytes.rewind();
			emitRecord(recordBytes, targetChannel);
		}
	}

	public void broadcastRecord(ByteBuffer recordBytes) throws IOException {
		do {
			final BufferBuilder bufferBuilder = getBufferBuilderForBroadcast();
			bufferBuilder.appendAndCommit(recordBytes);

			if (bufferBuilder.isFull()) {
				finishBroadcastBuffer();
			}
		}
		while (recordBytes.hasRemaining());
	}

	public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < currentPartitionBuffers.length; targetChannel++) {
				finishPartitionBuffer(targetChannel);

				// Retain the buffer so that it can be recycled by each channel of targetPartition
				subpartitions[targetChannel].add(eventBufferConsumer.copy(), isPriorityEvent);
			}

			flushAll();
		}
	}

	// ------------------------------------------------------------------------

	private BufferBuilder getBufferBuilderForSinglePartitionEmit(int targetpartition) throws IOException {
		final BufferBuilder current = currentPartitionBuffers[targetpartition];
		if (current != null) {
			return current;
		}

		return getNewBufferBuilderForSinglePartitionEmit(targetpartition);
	}

	private BufferBuilder getNewBufferBuilderForSinglePartitionEmit(int targetpartition) throws IOException {
		checkInProduceState();
		ensureUnicastMode();

		final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetpartition);
		currentPartitionBuffers[targetpartition] = bufferBuilder;
		subpartitions[targetpartition].add(bufferBuilder.createBufferConsumer(), false);
		return bufferBuilder;
	}

	private BufferBuilder getBufferBuilderForBroadcast() throws IOException {
		final BufferBuilder buffer = broadcastBuffer;
		if (buffer != null) {
			return buffer;
		}

		return getNewBufferBuilderForBroadcast();
	}

	private BufferBuilder getNewBufferBuilderForBroadcast() throws IOException {
		checkInProduceState();
		ensureBroadcastMode();

		final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
		broadcastBuffer = bufferBuilder;

		try (BufferConsumer consumer = bufferBuilder.createBufferConsumer()) {
			for (BufferWritingSubpartition subpartition : bufferWritingSubpartitions) {
				subpartition.add(consumer.copy(), false);
			}
		}

		return bufferBuilder;
	}

	private BufferBuilder requestNewBufferBuilderFromPool(int targetChannel) throws IOException {
		numBuffersOut.inc();

		final BufferBuilder builder = bufferPool.requestBufferBuilder(targetChannel);
		if (builder != null) {
			return builder;
		}

		final long start = System.currentTimeMillis();
		try {
			final BufferBuilder blockingBuffer = bufferPool.requestBufferBuilderBlocking(targetChannel);
			idleTimeMsPerSecond.markEvent(System.currentTimeMillis() - start);
			return blockingBuffer;
		} catch (InterruptedException e) {
			throw new IOException("Interruped while waiting for buffer");
		}
	}

	private void finishPartitionBuffer(int targetPartition) {
		currentPartitionBuffers[targetPartition] = null;
	}

	private void finishBroadcastBuffer() {
		broadcastBuffer = null;
	}

	private void ensureUnicastMode() {
		finishBroadcastBuffer();
	}

	private void ensureBroadcastMode() throws IOException {
		for (int i = 0; i < currentPartitionBuffers.length; i++) {
			finishPartitionBuffer(i);
		}
	}
}
