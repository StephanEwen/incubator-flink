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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.DuplicatedByteBuf;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ScatteringByteChannel;

/**
 * Minimal read-only {@link DuplicatedByteBuf} implementation wrapping a {@link NetworkBuffer},
 * similar to <tt>io.netty.buffer.DuplicatedAbstractByteBuf</tt> and
 * <tt>io.netty.buffer.ReadOnlyByteBuf</tt>.
 */
public final class DuplicatedNetworkBuffer extends DuplicatedByteBuf implements Buffer{

	/**
	 * Creates a buffer which shares the memory segment of the given buffer.
	 *
	 * <p>Modifying the content of a duplicate will affect the original buffer and vice versa while
	 * reader and writer indices and markers are not shared. Reference counters are shared but the
	 * duplicate is not {@link #retain() retained} automatically.
	 *
	 * @param buffer the buffer to duplicate
	 */
	DuplicatedNetworkBuffer(NetworkBuffer buffer) {
		super(buffer);
	}

	@Override
	public NetworkBuffer unwrap() {
		return (NetworkBuffer) super.unwrap();
	}

	@Override
	public DuplicatedNetworkBuffer duplicate() {
		DuplicatedNetworkBuffer duplicatedBuffer = new DuplicatedNetworkBuffer(unwrap());
		duplicatedBuffer.setIndex(readerIndex(), writerIndex());
		return duplicatedBuffer;
	}

	@Override
	public boolean isBuffer() {
		return unwrap().isBuffer();
	}

	@Override
	public void tagAsEvent() {
		unwrap().tagAsEvent();
	}

	@Override
	public MemorySegment getMemorySegment() {
		return unwrap().getMemorySegment();
	}

	@Override
	public void recycleBuffer() {
		unwrap().recycleBuffer();
	}

	@Override
	public boolean isRecycled() {
		return unwrap().isRecycled();
	}

	@Override
	public Buffer retainBuffer() {
		unwrap().retainBuffer();
		return this;
	}

	@Override
	public int getSize() {
		return unwrap().getSize();
	}

	@Override
	public int getReaderIndex() {
		return readerIndex();
	}

	@Override
	public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {
		readerIndex(readerIndex);
	}

	@Override
	public int getWriterIndex() {
		return writerIndex();
	}

	@Override
	public boolean setWriterIndex(int previousIdx, int newIdx) {
		throw new UnsupportedOperationException("Writer index should only be updated at the original buffer.");
	}

	@Override
	public Buffer sealAndDuplicate() {
		DuplicatedNetworkBuffer duplicatedBuffer = unwrap().sealAndDuplicate();
		duplicatedBuffer.setIndex(readerIndex(), writerIndex());
		return duplicatedBuffer	;
	}

	@Override
	public ByteBuffer getNioBufferReadable() {
		return nioBuffer();
	}

	@Override
	public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
		return nioBuffer(index, length);
	}

	@Override
	public boolean isWritable() {
		return false;
	}

	@Override
	public boolean isWritable(int numBytes) {
		return false;
	}

	@Override
	public void setAllocator(ByteBufAllocator allocator) {
		unwrap().setAllocator(allocator);
	}

	@Override
	protected byte _getByte(int index) {
		return unwrap()._getByte(index);
	}

	@Override
	protected short _getShort(int index) {
		return unwrap()._getShort(index);
	}

	@Override
	protected int _getUnsignedMedium(int index) {
		return unwrap()._getUnsignedMedium(index);
	}

	@Override
	protected int _getInt(int index) {
		return unwrap()._getInt(index);
	}

	@Override
	protected long _getLong(int index) {
		return unwrap()._getLong(index);
	}

	// ------------------------------------------------------------------------

	@Override
	public ByteBuf capacity(int newCapacity) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setBytes(int index, ByteBuffer src) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public int setBytes(int index, InputStream in, int length) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public int setBytes(int index, ScatteringByteChannel in, int length) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setByte(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setByte(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setShort(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setShort(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setMedium(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setMedium(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setInt(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setInt(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setLong(int index, long value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setLong(int index, long value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public byte[] array() {
		throw new ReadOnlyBufferException();
	}

	@Override
	public int arrayOffset() {
		throw new ReadOnlyBufferException();
	}

	@Override
	public boolean hasMemoryAddress() {
		return false;
	}

	@Override
	public long memoryAddress() {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf discardReadBytes() {
		throw new ReadOnlyBufferException();
	}

}
