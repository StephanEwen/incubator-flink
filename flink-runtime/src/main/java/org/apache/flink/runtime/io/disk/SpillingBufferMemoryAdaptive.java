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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memorymanager.AbstractPagedOutputView;
import org.apache.flink.runtime.memorymanager.AllocationCategory;
import org.apache.flink.runtime.memorymanager.DynamicMemoryConsumer;
import org.apache.flink.runtime.memorymanager.DynamicMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryAllocationException;
import org.apache.flink.runtime.memorymanager.MemoryAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An output view that buffers written data in memory pages and spills them when they are full.
 */
public class SpillingBufferMemoryAdaptive extends AbstractPagedOutputView implements DynamicMemoryConsumer {
	
	private static final Logger LOG = LoggerFactory.getLogger(SpillingBufferMemoryAdaptive.class);
	
	private static final int MIN_PAGES = 4;
	
	
	private final Object lock = new Object();	// securing segment transitions, asynchronous closing & spilling
	
	private final ArrayList<MemorySegment> fullSegments;
	
	private final MemoryAllocator memAllocator;
	
	private final IOManager ioManager;
	
	private BlockChannelWriter writer;
	
	private SpillableMemoryInputView inMemInView;
	
	private FileChannelInputView externalInView;
	
	private int blockCount;
	
	private int numBytesInLastSegment;

	// --------------------------------------------------------------------------------------------

	public SpillingBufferMemoryAdaptive(DynamicMemoryManager memManager, IOManager ioManager, AllocationCategory memoryCategory) throws MemoryAllocationException {
		super(memManager.getPageSize(), 0);
		
		this.fullSegments = new ArrayList<MemorySegment>(16);
		this.memAllocator = memManager.createMemoryAllocator(this, memoryCategory, MIN_PAGES);
		this.ioManager = ioManager;
		
		MemorySegment firstSegment = memAllocator.nextSegment();
		initializeFirstSegment(firstSegment);
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
		synchronized (lock) {
			if (writer == null) {
				fullSegments.add(current);
				
				MemorySegment nextSeg = memAllocator.nextSegment();
				if (nextSeg != null) {
					// more memory available
					return nextSeg;
				}
				else {
					// out of memory, need to spill
					spill();
					
					// now that we spilled, we can also return the memory
					return writer.getNextReturnedSegment();
				}
			}
		}
		
		// case else : spilling
		writer.writeBlock(current);
		blockCount++;
		return writer.getNextReturnedSegment();
	}
	
	
	public DataInputView rewindAndGetInput() throws IOException {
		synchronized (lock) {
			// check whether this is the first flip and we need to add the current segment to the full ones
			if (inMemInView == null && externalInView == null) {
				// first flip
				if (writer == null) {
					// in memory
					fullSegments.add(getCurrentSegment());
					numBytesInLastSegment = getCurrentPositionInSegment();
					inMemInView = new SpillableMemoryInputView(
							fullSegments, memAllocator, ioManager, segmentSize, numBytesInLastSegment, MIN_PAGES);
				}
				else {
					// external: write the last segment and collect the memory back
					writer.writeBlock(getCurrentSegment());
					numBytesInLastSegment = getCurrentPositionInSegment();
					blockCount++;
					
					writer.close();
					
					for (int i = 0; i < MIN_PAGES; i++) {
						memAllocator.returnSegment(writer.getNextReturnedSegment());
					}
				}
				
				// make sure we cannot write more
				clear();
			}
			
			// on all flips:
			if (writer == null) {
				// in memory
				inMemInView.reset();
				return inMemInView;
			}
			else {
				// recollect memory from a previous view
				if (externalInView != null) {
					externalInView.close();
				}
				
				BlockChannelReader reader = ioManager.createBlockChannelReader(writer.getChannelID());
				externalInView = new FileChannelInputView(reader, memAllocator, MIN_PAGES, numBytesInLastSegment);
				return externalInView;
			}
		}
	}
	

	public void close() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Closing adaptive spilling buffer.");
		}
		
		synchronized (lock) {
			// we clear all fields so that successive calls to any of the page transition methods cause errors
			try {
				fullSegments.clear();
				
				if (inMemInView != null) {
					try {
						inMemInView.close();
					}
					catch (Throwable t) {
						LOG.error("Could not remove spill file for adaptive spilling buffer.", t);
					}
					inMemInView = null;
				}
				
				// clean up the writer
				if (writer != null) {
					// closing before the first flip, collect the memory in the writer
					try {
						writer.closeAndDelete();
					}
					catch (Throwable t) {
						// if an error happens here in the closing phase only, it does not matter any more.
						// all previous writes / flips / reads were successful
						LOG.error("Could not remove spill file for adaptive spilling buffer.", t);
					}
					writer = null;
				}
				
				// clean up the views
				if (externalInView != null) {
					try {
						externalInView.closeAndDelete();
					}
					catch (Throwable t) {
						// if an error happens here in the closing phase only, it does not matter any more.
						// all previous writes / flips / reads were successful
						if (LOG.isDebugEnabled()) {
							LOG.debug("Could not remove spill file for adaptive spilling buffer reader.", t);
						}
					}
					externalInView = null;
				}
			}
			finally {
				// very important
				memAllocator.closeAndReleaseAll();
			}
		}
	}
	
	public boolean isInMemory() {
		return writer == null && (inMemInView == null || inMemInView.isInMemory());
	}

	/**
	 * This method spills the buffer during its write phase
	 */
	private void spill() throws IOException {
		if (fullSegments.size() <= MIN_PAGES) {
			throw new IllegalStateException("Spilling buffer holds only the minimal memory, cannot spill.");
		}
		
		if (writer == null) {
			writer = ioManager.createBlockChannelWriter(ioManager.createChannel());
			
			// add all segments to the writer
			blockCount = fullSegments.size();
			
			for (int i = 0; i < fullSegments.size(); i++) {
				writer.writeBlock(fullSegments.get(i));
			}
			fullSegments.clear();
			fullSegments.trimToSize();
			
			// return all except our minimum buffers
			final int toReturn = blockCount - MIN_PAGES;
			for (int i = 0; i < toReturn; i++) {
				memAllocator.releaseSegment(writer.getNextReturnedSegment());
			}
		}
		else {
			throw new IllegalStateException("spilled already");
		}
	}


	// --------------------------------------------------------------------------------------------
	//  Adaptive memory consumer logic
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void releaseMemory(int numPages) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Triggering full spill for spilling buffer.");
		}
		
		// this buffer can only spill all or nothing
		synchronized (lock) {
			if (inMemInView == null && externalInView == null) {
				// write phase
				if (writer == null) {
					spill();
				}
				// else spilled already
			}
			else if (inMemInView != null) {
				// read phase with in mem view
				inMemInView.spill();
			}
			// else: spilled already during (or after) writing
		}
	}

	@Override
	public void notifyMoreMemoryAvailable(int numPages) {
		// this one cannot react to memory becoming available at some later stage
	}
}
