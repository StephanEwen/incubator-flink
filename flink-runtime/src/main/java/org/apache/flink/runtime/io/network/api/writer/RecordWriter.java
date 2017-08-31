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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.util.Random;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartition} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flush()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> {

	protected final ResultPartition targetPartition;

	private final ChannelSelector<T> channelSelector;

	private final int numChannels;

	/** {@link RecordSerializer} per outgoing channel */
	private final RecordSerializer<T>[] serializers;

	private final Random RNG = new XORShiftRandom();

	private Counter numBytesOut = new SimpleCounter();

	public RecordWriter(ResultPartition partition) {
		this(partition, new RoundRobinChannelSelector<T>());
	}

	@SuppressWarnings("unchecked")
	public RecordWriter(ResultPartition partition, ChannelSelector<T> channelSelector) {
		this.targetPartition = partition;
		this.channelSelector = channelSelector;

		this.numChannels = partition.getNumberOfSubpartitions();

		/**
		 * The runtime exposes a channel abstraction for the produced results
		 * (see {@link ChannelSelector}). Every channel has an independent
		 * serializer.
		 */
		this.serializers = new SpanningRecordSerializer[numChannels];
		for (int i = 0; i < numChannels; i++) {
			serializers[i] = new SpanningRecordSerializer<T>();
		}
	}

	public void emit(T record) throws IOException, InterruptedException {
		for (int targetChannel : channelSelector.selectChannels(record, numChannels)) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to broadcast Streaming Watermarks in-band with records. This ignores
	 * the {@link ChannelSelector}.
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to send LatencyMarks to a random target channel
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		sendToTarget(record, RNG.nextInt(numChannels));
	}

	private void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		RecordSerializer<T> serializer = serializers[targetChannel];

		synchronized (serializer) {
			NetworkBuffer buffer = (NetworkBuffer) serializer.getCurrentBuffer();
			int writerIndexBefore = (buffer == null) ? 0 : buffer.getWriterIndex();

			SerializationResult result = serializer.addRecord(record);

			// serialize any copy into network buffers until the whole record has been written
			do {
				if (buffer == null) {
					// a serializer without a current target buffer should return this result:
					checkState(result == SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL);

					buffer = (NetworkBuffer) targetPartition.getBufferProvider().requestBufferBlocking();
					writerIndexBefore = 0;
					result = serializer.setNextBuffer(buffer);
				}

				// written anything?
				if (buffer.getWriterIndex() > writerIndexBefore) {
					// add to target sub-partition so the network stack can already read from it
					// while we're completing the buffer (therefore, also retain it)
					buffer.retain();
					targetPartition.add(buffer, targetChannel);
					numBytesOut.inc(buffer.getWriterIndex() - writerIndexBefore);
				}

				// make room for a new buffer if this one is full
				if (result.isFullBuffer()) {
					serializer.clearCurrentBuffer();
					buffer.recycle();
					buffer = null;
				}
			} while (!result.isFullRecord());

			// NOTE: cleanup of the target buffers in cases of failures is handled some layers above
			//       which (need to) call #clearBuffers() to also reset the serializers!
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		final Buffer eventBuffer = EventSerializer.toBuffer(event);
		try {
			for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
				RecordSerializer<T> serializer = serializers[targetChannel];

				synchronized (serializer) {
					Buffer buffer = serializer.getCurrentBuffer();
					if (buffer != null) {
						serializer.clearCurrentBuffer();
						buffer.recycle();
					} else if (serializer.hasData()) {
						// sanity check
						throw new IllegalStateException("No buffer, but serializer has buffered data.");
					}

					// retain the buffer so that it can be recycled by each channel of targetPartition
					eventBuffer.retain();
					targetPartition.add(eventBuffer, targetChannel);
				}
			}
		} finally {
			// we do not need to further retain the eventBuffer
			// (it will be recycled after the last channel stops using it)
			eventBuffer.recycle();
		}
	}

	public void flush() throws IOException {
	}

	public void clearBuffers() {
		for (RecordSerializer<?> serializer : serializers) {
			synchronized (serializer) {
				try {
					Buffer buffer = serializer.getCurrentBuffer();

					if (buffer != null) {
						buffer.recycle();
					}
				}
				finally {
					serializer.clear();
				}
			}
		}
	}

	/**
	 * Sets the metric group for this RecordWriter.
	 * @param metrics
     */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		numBytesOut = metrics.getNumBytesOutCounter();
	}

}
