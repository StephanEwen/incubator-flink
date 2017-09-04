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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToMessageDecoder;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple and generic interface to serialize messages to Netty's buffer space.
 */
abstract class NettyMessage {

	// ------------------------------------------------------------------------
	// Note: Every NettyMessage subtype needs to have a public 0-argument
	// constructor in order to work with the generic deserializer.
	// ------------------------------------------------------------------------

	static final int HEADER_LENGTH = 4 + 4 + 1; // frame length (4), magic number (4), msg ID (1)

	static final int MAGIC_NUMBER = 0xBADC0FFE;

	abstract ByteBuf write(ByteBufAllocator allocator) throws Exception;

	// ------------------------------------------------------------------------

	/**
	 * Allocates a new (header and contents) buffer and adds some header information for the frame
	 * decoder.
	 *
	 * <p>Before sending the buffer, you must write the actual length after adding the contents as
	 * an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageDecoder}
	 */
	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
		return allocateBuffer(allocator, id, -1);
	}

	/**
	 * Allocates a new (header and contents) buffer and adds some header information for the frame
	 * decoder.
	 *
	 * <p>If the <tt>length</tt> is unknown, you must write the actual length after adding the
	 * contents as an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 * @param length
	 * 		content length (or <tt>-1</tt> if unknown)
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageDecoder}
	 */
	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int length) {
		return allocateBuffer(allocator, id, 0, length, true);
	}

	/**
	 * Allocates a new buffer and adds some header information for the frame decoder.
	 *
	 * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
	 * the contents as an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 * @param headerLength
	 * 		additional header length that should be part of the allocated buffer
	 * @param contentLength
	 * 		content length (or <tt>-1</tt> if unknown)
	 * @param allocateForContent
	 * 		whether to make room for the actual content in the buffer (<tt>true</tt>) or whether to
	 * 		only return a buffer with the header information (<tt>false</tt>)
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageDecoder}
	 */
	private static ByteBuf allocateBuffer(
		ByteBufAllocator allocator, byte id, int headerLength, int contentLength,
		boolean allocateForContent) {
		Preconditions.checkArgument(contentLength <= Integer.MAX_VALUE - HEADER_LENGTH);

		final ByteBuf buffer;
		if (!allocateForContent) {
			buffer = allocator.directBuffer(HEADER_LENGTH + headerLength);
		} else if (contentLength != -1) {
			buffer = allocator.directBuffer(HEADER_LENGTH + headerLength + contentLength);
		} else {
			// content length unknown -> start with the default initial size (rather than HEADER_LENGTH only):
			buffer = allocator.directBuffer();
		}
		buffer.writeInt(HEADER_LENGTH + headerLength + contentLength); // may be updated later, e.g. if contentLength == -1
		buffer.writeInt(MAGIC_NUMBER);
		buffer.writeByte(id);

		return buffer;
	}

	// ------------------------------------------------------------------------
	// Generic NettyMessage encoder and decoder
	// ------------------------------------------------------------------------

	@ChannelHandler.Sharable
	static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {

		@Override
		public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
			if (msg instanceof NettyMessage) {

				ByteBuf serialized = null;

				try {
					serialized = ((NettyMessage) msg).write(ctx.alloc());
				}
				catch (Throwable t) {
					throw new IOException("Error while serializing message: " + msg, t);
				}
				finally {
					if (serialized != null) {
						ctx.write(serialized, promise);
					} else {
						// not forwarded to netty (already sent) -
						promise.setSuccess();
					}
				}
			}
			else {
				ctx.write(msg, promise);
			}
		}

		// Create the frame length decoder here as it depends on the encoder
		//
		// +------------------+------------------+--------++----------------+
		// | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
		// +------------------+------------------+--------++----------------+
		static LengthFieldBasedFrameDecoder createFrameLengthDecoder() {
			return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, -4, 4);
		}
	}

	@ChannelHandler.Sharable
	static class NettyMessageDecoder extends MessageToMessageDecoder<ByteBuf> {

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			int magicNumber = msg.readInt();

			if (magicNumber != MAGIC_NUMBER) {
				throw new IllegalStateException("Network stream corrupted: received incorrect magic number.");
			}

			byte msgId = msg.readByte();

			final NettyMessage decodedMsg;
			switch (msgId) {
				case BufferResponse.ID:
					decodedMsg = BufferResponse.readFrom(msg);
					break;
				case PartitionRequest.ID:
					decodedMsg = PartitionRequest.readFrom(msg);
					break;
				case TaskEventRequest.ID:
					decodedMsg = TaskEventRequest.readFrom(msg, getClass().getClassLoader());
					break;
				case ErrorResponse.ID:
					decodedMsg = ErrorResponse.readFrom(msg);
					break;
				case CancelPartitionRequest.ID:
					decodedMsg = CancelPartitionRequest.readFrom(msg);
					break;
				case CloseRequest.ID:
					decodedMsg = CloseRequest.readFrom(msg);
					break;
				default:
					throw new IllegalStateException("Received unknown message from producer: " + msg);
			}

			out.add(decodedMsg);
		}
	}

	// ------------------------------------------------------------------------
	// Server responses
	// ------------------------------------------------------------------------

	static class BufferResponse extends NettyMessage {

		private static final byte ID = 0;

		final ByteBuf buffer;

		final InputChannelID receiverId;

		final int sequenceNumber;

		final boolean isBuffer;

		BufferResponse(NetworkBuffer buffer, int sequenceNumber, InputChannelID receiverId) {
			this.buffer = checkNotNull(buffer);
			this.sequenceNumber = sequenceNumber;
			this.receiverId = receiverId;
			this.isBuffer = buffer.isBuffer();
		}

		BufferResponse(ByteBuf buffer, boolean isBuffer, int sequenceNumber, InputChannelID receiverId) {
			this.buffer = checkNotNull(buffer);
			this.sequenceNumber = sequenceNumber;
			this.receiverId = receiverId;
			this.isBuffer = isBuffer;
		}

		boolean isBuffer() {
			return isBuffer;
		}

		ByteBuf getNettyBuffer() {
			return buffer;
		}

		void releaseBuffer() {
			buffer.release();
		}

		// --------------------------------------------------------------------
		// Serialization
		// --------------------------------------------------------------------

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			// do not send buffers with no (readable) data
			return write(allocator, false);
		}

		@VisibleForTesting
		final ByteBuf write(ByteBufAllocator allocator, boolean allowEmpty) throws IOException {
			int headerLength = 16 + 4 + 1 + 4;

			ByteBuf headerBuf = null;
			try {
				if (buffer instanceof NetworkBuffer) {
					// in order to forward the buffer to netty, it needs an allocator set
					((NetworkBuffer) buffer).setAllocator(allocator);
				}

				// define the part of the buffer that is ready to send
				int readerIndex = buffer.readerIndex();
				int readableBytes = buffer.readableBytes();
				if (!allowEmpty && readableBytes == 0) {
					buffer.release(); // not forwarding buffer, therefore we need to recycle it!
					return null;
				}
				ByteBuf readyToSend = buffer.slice(readerIndex, readableBytes);
				buffer.readerIndex(readerIndex + readableBytes);

				// only allocate header buffer - we will combine it with the data buffer below
				headerBuf = allocateBuffer(allocator, ID, headerLength, readableBytes, false);

				receiverId.writeTo(headerBuf);
				headerBuf.writeInt(sequenceNumber);
				headerBuf.writeBoolean(isBuffer);
				headerBuf.writeInt(readableBytes);

				CompositeByteBuf composityBuf = allocator.compositeBuffer();
				composityBuf.addComponent(headerBuf);
				composityBuf.addComponent(readyToSend);
				// update writer index since we have written data to the components:
				composityBuf.writerIndex(headerBuf.writerIndex() + readyToSend.writerIndex());
				return composityBuf;
			}
			catch (Throwable t) {
				if (headerBuf != null) {
					headerBuf.release();
				}
				buffer.release();

				throw new IOException(t);
			}
		}

		static BufferResponse readFrom(ByteBuf buffer) {
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
			int sequenceNumber = buffer.readInt();
			boolean isBuffer = buffer.readBoolean();
			int size = buffer.readInt();

			ByteBuf retainedSlice = buffer.readSlice(size).retain();
			return new BufferResponse(retainedSlice, isBuffer, sequenceNumber, receiverId);
		}
	}

	static class ErrorResponse extends NettyMessage {

		private static final byte ID = 1;

		Throwable cause;

		InputChannelID receiverId;

		ErrorResponse(Throwable cause) {
			this.cause = cause;
		}

		ErrorResponse(Throwable cause, InputChannelID receiverId) {
			this.cause = cause;
			this.receiverId = receiverId;
		}

		boolean isFatalError() {
			return receiverId == null;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			final ByteBuf result = allocateBuffer(allocator, ID);

			try (ObjectOutputStream oos = new ObjectOutputStream(new ByteBufOutputStream(result))) {
				oos.writeObject(cause);

				if (receiverId != null) {
					result.writeBoolean(true);
					receiverId.writeTo(result);
				} else {
					result.writeBoolean(false);
				}

				// Update frame length...
				result.setInt(0, result.readableBytes());
				return result;
			}
			catch (Throwable t) {
				result.release();

				if (t instanceof IOException) {
					throw (IOException) t;
				} else {
					throw new IOException(t);
				}
			}
		}

		static ErrorResponse readFrom(ByteBuf buffer) throws Exception {
			try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
				Object obj = ois.readObject();

				if (!(obj instanceof Throwable)) {
					throw new ClassCastException("Read object expected to be of type Throwable, " +
							"actual type is " + obj.getClass() + ".");
				} else {
					ErrorResponse result = new ErrorResponse((Throwable) obj);

					if (buffer.readBoolean()) {
						result.receiverId = InputChannelID.fromByteBuf(buffer);
					}
					return result;
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	// Client requests
	// ------------------------------------------------------------------------

	static class PartitionRequest extends NettyMessage {

		final static byte ID = 2;

		ResultPartitionID partitionId;

		int queueIndex;

		InputChannelID receiverId;

		private PartitionRequest() {
		}

		PartitionRequest(ResultPartitionID partitionId, int queueIndex, InputChannelID receiverId) {
			this.partitionId = partitionId;
			this.queueIndex = queueIndex;
			this.receiverId = receiverId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16 + 16 + 4 + 16);

				partitionId.getPartitionId().writeTo(result);
				partitionId.getProducerId().writeTo(result);
				result.writeInt(queueIndex);
				receiverId.writeTo(result);

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
		}

		static PartitionRequest readFrom(ByteBuf buffer) {
			PartitionRequest result = new PartitionRequest();

			result.partitionId = new ResultPartitionID(IntermediateResultPartitionID.fromByteBuf(buffer), ExecutionAttemptID.fromByteBuf(buffer));
			result.queueIndex = buffer.readInt();
			result.receiverId = InputChannelID.fromByteBuf(buffer);

			return result;
		}

		@Override
		public String toString() {
			return String.format("PartitionRequest(%s:%d)", partitionId, queueIndex);
		}
	}

	static class TaskEventRequest extends NettyMessage {

		final static byte ID = 3;

		TaskEvent event;

		InputChannelID receiverId;

		ResultPartitionID partitionId;

		private TaskEventRequest() {
		}

		TaskEventRequest(TaskEvent event, ResultPartitionID partitionId, InputChannelID receiverId) {
			this.event = event;
			this.receiverId = receiverId;
			this.partitionId = partitionId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				// TODO Directly serialize to Netty's buffer
				ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

				result = allocateBuffer(allocator, ID, 4 + serializedEvent.remaining() + 16 + 16 + 16);

				result.writeInt(serializedEvent.remaining());
				result.writeBytes(serializedEvent);

				partitionId.getPartitionId().writeTo(result);
				partitionId.getProducerId().writeTo(result);

				receiverId.writeTo(result);

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
		}

		static TaskEventRequest readFrom(ByteBuf buffer, ClassLoader classLoader) throws IOException {
			TaskEventRequest result = new TaskEventRequest();

			// directly deserialize fromNetty's buffer
			int length = buffer.readInt();
			ByteBuffer serializedEvent = buffer.nioBuffer();
			// assume this event's content is read from the ByteBuf (positions are not shared!)
			buffer.readerIndex(buffer.readerIndex() + length);

			result.event = (TaskEvent) EventSerializer.fromSerializedEvent(serializedEvent, classLoader);

			result.partitionId = new ResultPartitionID(IntermediateResultPartitionID.fromByteBuf(buffer), ExecutionAttemptID.fromByteBuf(buffer));

			result.receiverId = InputChannelID.fromByteBuf(buffer);

			return result;
		}
	}

	/**
	 * Cancels the partition request of the {@link InputChannel} identified by
	 * {@link InputChannelID}.
	 *
	 * <p> There is a 1:1 mapping between the input channel and partition per physical channel.
	 * Therefore, the {@link InputChannelID} instance is enough to identify which request to cancel.
	 */
	static class CancelPartitionRequest extends NettyMessage {

		final static byte ID = 4;

		InputChannelID receiverId;

		CancelPartitionRequest(InputChannelID receiverId) {
			this.receiverId = receiverId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws Exception {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16);
				receiverId.writeTo(result);
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}

			return result;
		}

		static CancelPartitionRequest readFrom(ByteBuf buffer) throws Exception {
			return new CancelPartitionRequest(InputChannelID.fromByteBuf(buffer));
		}
	}

	static class CloseRequest extends NettyMessage {

		private static final byte ID = 5;

		CloseRequest() {
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws Exception {
			return allocateBuffer(allocator, ID, 0);
		}

		static CloseRequest readFrom(@SuppressWarnings("unused") ByteBuf buffer) throws Exception {
			return new CloseRequest();
		}
	}
}
