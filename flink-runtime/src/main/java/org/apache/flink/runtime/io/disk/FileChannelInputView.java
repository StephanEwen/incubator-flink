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

package org.apache.flink.runtime.io.disk;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.memorymanager.AbstractPagedInputView;
import org.apache.flink.runtime.memorymanager.MemoryAllocator;
import org.apache.flink.runtime.util.MathUtils;

/**
 * A {@link org.apache.flink.core.memory.DataInputView} that is backed by a {@link BlockChannelReader},
 * making it effectively a data input stream. The view reads it data in blocks from the underlying channel.
 * The view can read data that has been written by a {@link FileChannelOutputView}, or that was written in blocks
 * in another fashion.
 */
public class FileChannelInputView extends AbstractPagedInputView {
	
	private final BlockChannelReader reader;
	
	private final MemoryAllocator memAllocator;
	
	private final List<MemorySegment> memory;
	
	private final int sizeOfLastBlock;
	
	private int numRequestsRemaining;
	
	private int numBlocksRemaining;
	
	// --------------------------------------------------------------------------------------------
	
	public FileChannelInputView(BlockChannelReader reader, MemoryAllocator allocator, int numBlocksToUse, int sizeOfLastBlock) throws IOException {
		super(0);
		
		checkNotNull(reader);
		checkNotNull(allocator);
		checkArgument(!reader.isClosed());
		
		this.reader = reader;
		this.memAllocator = allocator;
		this.memory = new ArrayList<MemorySegment>(numBlocksToUse);
		this.sizeOfLastBlock = sizeOfLastBlock;
		
		// check that this allocator will always allow us to hold onto that number of memory segments
		if (allocator.getGuaranteedNumberOfSegments() < numBlocksToUse) {
			throw new IllegalArgumentException("The FileChannelInputView cannot use more memory segments than its memory allocator has reserved.");
		}
		
		try {
			for (int i = 0; i < numBlocksToUse; i++) {
				MemorySegment next = allocator.nextSegment();
				
				// if we cannot get our memory, abort
				if (next == null) {
					throw new IOException("Cannot acquire memory for FileChannelInputView");
				}
				memory.add(next);
			}
			
			final long channelLength = reader.getSize();
			final int segmentSize = allocator.getMemorySegmentSize();
			
			this.numBlocksRemaining = MathUtils.checkedDownCast(channelLength / segmentSize);
			if (channelLength % segmentSize != 0) {
				this.numBlocksRemaining++;
			}
			
			this.numRequestsRemaining = numBlocksRemaining;
			
			for (int i = 0; i < memory.size(); i++) {
				sendReadRequest(memory.get(i));
			}
			
			advance();
		}
		catch (IOException e) {
			allocator.releaseSegments(memory);
			throw e;
		}
	}
	
	public void close() throws IOException {
		close(false);
	}
	
	public void closeAndDelete() throws IOException {
		close(true);
	}
	
	private void close(boolean deleteFile) throws IOException {
		try {
			clear();
			if (deleteFile) {
				reader.closeAndDelete();
			} else {
				reader.close();
			}
		} finally {
			synchronized (memory) {
				memAllocator.releaseSegments(memory);
				memory.clear();
			}
		}
	}
	
	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException {
		// check for end-of-stream
		if (numBlocksRemaining <= 0) {
			reader.close();
			throw new EOFException();
		}
		
		// send a request first. if we have only a single segment, this same segment will be the one obtained in the next lines
		if (current != null) {
			sendReadRequest(current);
		}
		
		// get the next segment
		numBlocksRemaining--;
		return reader.getNextReturnedSegment();
	}
	
	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return numBlocksRemaining > 0 ? segment.size() : sizeOfLastBlock;
	}
	
	private void sendReadRequest(MemorySegment seg) throws IOException {
		if (numRequestsRemaining > 0) {
			reader.readBlock(seg);
			numRequestsRemaining--;
		} else {
			memAllocator.releaseSegment(seg);
		}
	}
}
