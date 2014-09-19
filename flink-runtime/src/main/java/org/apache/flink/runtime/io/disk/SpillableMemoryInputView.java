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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriterWithCallback;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memorymanager.AbstractPagedInputView;
import org.apache.flink.runtime.memorymanager.MemoryAllocator;

import com.google.common.base.Preconditions;

/**
 * A readable data buffer, backed by a collection of memory segments that can be spilled at any
 * time during reading.
 */
public class SpillableMemoryInputView extends AbstractPagedInputView {
	
	private final Object lock = new Object();
	
	private final ArrayList<MemorySegment> segments;
	
	private final IOManager ioManager;
	
	private final MemoryAllocator memAllocator;
	
	private BlockChannelReader reader;
	
	
	private int currentSegmentIndex;
	
	private final int numSegments;
	
	private int numIORequestsRemaining;
	
	private final int segmentSize;
	
	private final int limitInLastSegment;
	
	private final int numBuffersToRetain;
	
	
	public SpillableMemoryInputView(ArrayList<MemorySegment> segments, MemoryAllocator allocator, IOManager ioManager,
			int segmentSize, int limitInLastSegment, int numBuffersToRetain)
	{
		super(segments.get(0), segments.size() > 1 ? segmentSize : limitInLastSegment, 0);
		
		Preconditions.checkNotNull(allocator);
		Preconditions.checkNotNull(ioManager);
		Preconditions.checkArgument(numBuffersToRetain >= 1);
		
		this.segments = segments;
		this.memAllocator = allocator;
		this.ioManager = ioManager;
		
		this.currentSegmentIndex = 0;
		this.segmentSize = segmentSize;
		this.numSegments = segments.size();
		this.limitInLastSegment = limitInLastSegment;
		this.numBuffersToRetain = numBuffersToRetain;
	}

	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException {
		synchronized (lock) {
			if (reader == null) {
				// in memory case
				if (++currentSegmentIndex < numSegments) {
					return segments.get(currentSegmentIndex);
				} else {
					throw new EOFException();
				}
			}
			else {
				// spilled case
				// check for end-of-stream
				if (++currentSegmentIndex >= numSegments) {
					reader.close();
					throw new EOFException();
				}
				
				// send a request first.
				// if we have only a single segment, this same segment will be the one obtained in the next lines
				if (current != null) {
					sendReadRequest(current);
				}
				
				// get the next segment
				return reader.getNextReturnedSegment();
			}
		}
	}

	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return currentSegmentIndex == numSegments - 1 ? limitInLastSegment : segmentSize;
	}
	
	public void reset() throws IOException {
		synchronized (lock) {
			if (reader == null) {
				// in memory case
				currentSegmentIndex = 0;
				setFirstBuffer(segments.get(0));
			}
			else {
				// close the current reader - this waits for pending requests to return,
				// if we close before fully consuming the reader
				reader.close();
				
				// re-create the reader and the tracking variables
				reader = ioManager.createBlockChannelReader(reader.getChannelID());
				currentSegmentIndex = 0;
				numIORequestsRemaining = numSegments;
				
				// issue the initial read requests and wait for the first one to come back
				for (MemorySegment buffer : segments) {
					sendReadRequest(buffer);
				}
				
				setFirstBuffer(reader.getNextReturnedSegment());
			}
		}
	}
	
	public void spill() throws IOException {
		synchronized (lock) {
			// first write the whole thing
			// the close call waits for the writes to complete (for asynchronous implementations)
			BlockChannelWriterWithCallback writer = ioManager.createBulkBlockChannelWriter(ioManager.createChannel());
			
			for (MemorySegment seg : segments) {
				writer.writeBlock(seg);
			}
			writer.close();
			
			// create the reader
			reader = ioManager.createBlockChannelReader(writer.getChannelID());
			
			// collect the memory we wish to retain
			int toRetain = Math.min(numSegments, numBuffersToRetain);
			ArrayList<MemorySegment> toRetainList = new ArrayList<MemorySegment>(toRetain);
			MemorySegment currentSegment = null;
			
			if (currentSegmentIndex < numSegments) {
				// within a read, we need to hold on to the current segment
				currentSegment = segments.remove(currentSegmentIndex);
				toRetainList.add(currentSegment);
			}
			
			while (toRetainList.size() < toRetain) {
				toRetainList.add(segments.remove(segments.size() - 1));
			}
			
			// release all other memory
			memAllocator.releaseSegments(segments);
			segments.clear();
			segments.addAll(toRetainList);
			
			// if we are currently within a read, set the state of the reader to continue from there
			// the state of the input view should be valid still
			if (currentSegment != null) {
				int segmentsDone = currentSegmentIndex + 1;
				
				numIORequestsRemaining = numSegments - segmentsDone;
				reader.seekToPosition(segmentsDone * segmentSize);
				
				for (int i = 1; i < toRetain; i++) {
					sendReadRequest(segments.get(i));
				}
			}
			else {
				numIORequestsRemaining = 0;
			}
		}
	}
	
	public void close() throws IOException {
		// make sure the temp file is deleted
		BlockChannelReader reader = this.reader;
		if (reader != null) {
			this.reader = null;
			reader.closeAndDelete();
		}
		
		memAllocator.releaseSegments(segments);
	}
	
	public boolean isInMemory() {
		return reader == null;
	}
	
	private void sendReadRequest(MemorySegment seg) throws IOException {
		if (numIORequestsRemaining > 0) {
			reader.readBlock(seg);
			numIORequestsRemaining--;
		}
	}
	
	private void setFirstBuffer(MemorySegment seg) {
		seekInput(seg, 0, numSegments > 1 ? segmentSize : limitInLastSegment);
	}
}
