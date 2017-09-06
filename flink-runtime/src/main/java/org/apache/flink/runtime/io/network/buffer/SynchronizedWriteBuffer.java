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

/**
 * Subset of the {@link Buffer} methods for synchronized writers and readers where the writer always
 * uses {@link #setWriterIndex(int, int)} to update the writer index after a write
 * operation and the reader consumes the buffer only once using {@link
 * #sealAndDuplicate()}.
 *
 * <p>Note that the writer is safe to create duplicates but the reader may only do so via
 * {@link #sealAndDuplicate()}!
 *
 * <p>Note that multiple writers are not supported.
 */
public interface SynchronizedWriteBuffer {

	/**
	 * Releases this buffer once, i.e. reduces the reference count and recycles the buffer if the
	 * reference count reaches <tt>0</tt>.
	 *
	 * @see #retainBuffer()
	 */
	void recycleBuffer();

	/**
	 * Returns whether this buffer has been recycled or not.
	 *
	 * @return <tt>true</tt> if already recycled, <tt>false</tt> otherwise
	 */
	boolean isRecycled();

	/**
	 * Retains this buffer for further use, increasing the reference counter by <tt>1</tt>.
	 *
	 * @return <tt>this</tt> instance (for chained calls)
	 *
	 * @see #recycleBuffer()
	 */
	Buffer retainBuffer();

	/**
	 * Returns the underlying memory segment.
	 *
	 * @return the memory segment backing this buffer
	 */
	MemorySegment getMemorySegment();

	/**
	 * Returns the size of the buffer, i.e. the capacity of the underlying {@link MemorySegment}.
	 *
	 * @return size of the buffer
	 */
	int getSize();

	/**
	 * Returns the <tt>writer index</tt> of this buffer.
	 *
	 * <p>This is where writable bytes start in the backing memory segment.
	 *
	 * @return writer index (from 0 (inclusive) to the size of the backing {@link MemorySegment}
	 * (inclusive))
	 */
	int getWriterIndex();

	/**
	 * Tries to modify the writer index (in a synchronized way) assuming a previous index is known.
	 * The writer index will only be updated if the previous index still matches and is not invalid
	 * (from a call to {@link #sealAndDuplicate()}).
	 *
	 * @param previousIdx
	 * 		the previous index
	 * @param newIdx
	 * 		the (new) index to set
	 *
	 * @return whether the index was updated (<tt>true</tt>) or not (<tt>false</tt>)
	 *
	 * @see #sealAndDuplicate()
	 */
	boolean setWriterIndex(int previousIdx, int newIdx);

	/**
	 * In conjunction with {@link #setWriterIndex(int, int)}, this offers a synchronized
	 * creation of a duplicate with no visibility issues, ensuring the writer stops writing and is
	 * aware of the buffer being sealed.
	 *
	 * @return a duplicate buffer with the (synchronized) writer index as its writer index
	 *
	 * @see #setWriterIndex(int, int)
	 */
	Buffer sealAndDuplicate();

}
