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

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;

/**
 * A readerQueue of partition queues, which listens for channel writability changed
 * events before writing and flushing {@link Buffer} instances.
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

	private final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	private final Queue<SequenceNumberingViewReader> readerQueue = new ArrayDeque<>();

	private final Set<InputChannelID> released = Sets.newHashSet();

	private boolean fatalError;

	private ChannelHandlerContext ctx;

	@Override
	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelRegistered(ctx);
	}

	void notifyReaderNonEmpty(final SequenceNumberingViewReader reader) {
		ctx.executor().execute(new Runnable() {
			@Override
			public void run() {
				ctx.pipeline().fireUserEventTriggered(reader);
			}
		});
	}

	public void cancel(InputChannelID receiverId) {
		ctx.pipeline().fireUserEventTriggered(receiverId);
	}

	public void close() {
		if (ctx != null) {
			ctx.channel().close();
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg.getClass() == SequenceNumberingViewReader.class) {
			boolean triggerWrite = readerQueue.isEmpty();
			readerQueue.add((SequenceNumberingViewReader) msg);
			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel());
			}
		} else if (msg.getClass() == InputChannelID.class) {
			InputChannelID toCancel = (InputChannelID) msg;

			if (released.contains(toCancel)) {
				return;
			}

			// Cancel the request for the input channel
			int size = readerQueue.size();
			for (int i = 0; i < size; i++) {
				SequenceNumberingViewReader reader = readerQueue.poll();
				if (reader.getReceiverId().equals(toCancel)) {
					reader.releaseAllResources();
					markAsReleased(reader.getReceiverId());
				} else {
					readerQueue.add(reader);
				}
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
		if (fatalError) {
			return;
		}

		BufferAndAvailability next = null;

		try {
			if (channel.isWritable()) {
				while (true) {
					SequenceNumberingViewReader reader = readerQueue.poll();

					// No queue with available data
					if (reader == null) {
						return;
					}

					next = reader.getNextBuffer();

					if (next == null) {
						if (reader.isReleased()) {
							markAsReleased(reader.getReceiverId());
							Throwable cause = reader.getFailureCause();

							if (cause != null) {
								ErrorResponse msg = new ErrorResponse(
									new ProducerFailedException(cause),
									reader.getReceiverId());

								ctx.writeAndFlush(msg);
							}
						} // IllegalState?
					} else {
						if (next.moreAvailable()) {
							readerQueue.add(reader);
						}

						BufferResponse msg = new BufferResponse(
							next.buffer(),
							reader.getSequenceNumber(),
							reader.getReceiverId());

						if (isEndOfPartitionEvent(next.buffer())) {
							reader.notifySubpartitionConsumed();
							reader.releaseAllResources();

							markAsReleased(reader.getReceiverId());
						}

						channel.writeAndFlush(msg).addListener(writeListener);

						return;
					}
				}
			}
		} catch (Throwable t) {
			if (next != null) {
				next.buffer().recycle();
			}

			throw new IOException(t.getMessage(), t);
		}
	}

	private boolean isEndOfPartitionEvent(Buffer buffer) throws IOException {
		return !buffer.isBuffer() && EventSerializer.fromBuffer(buffer, getClass().getClassLoader()).getClass() == EndOfPartitionEvent.class;
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		releaseAllResources();

		ctx.fireChannelInactive();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		handleException(ctx.channel(), cause);
	}

	private void handleException(Channel channel, Throwable cause) throws IOException {
		fatalError = true;
		releaseAllResources();

		if (channel.isActive()) {
			channel.writeAndFlush(new ErrorResponse(cause)).addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void releaseAllResources() throws IOException {
		SequenceNumberingViewReader reader;
		while ((reader = readerQueue.poll()) != null) {
			reader.releaseAllResources();
			markAsReleased(reader.getReceiverId());
		}
	}

	/**
	 * Marks a receiver as released.
	 */
	private void markAsReleased(InputChannelID receiverId) {
		released.add(receiverId);
	}

	// This listener is called after an element of the current readerQueue has been
	// flushed. If successful, the listener triggers further processing of the
	// queues.
	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					handleException(future.channel(), future.cause());
				} else {
					handleException(future.channel(), new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				handleException(future.channel(), t);
			}
		}
	}
}
