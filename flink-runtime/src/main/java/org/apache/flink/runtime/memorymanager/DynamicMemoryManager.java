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

package org.apache.flink.runtime.memorymanager;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Set;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.util.MathUtils;


public class DynamicMemoryManager {
	
	/** The default memory page size. Currently set to 32 KiBytes. */
	public static final int DEFAULT_PAGE_SIZE = 32 * 1024;
	
	/** The minimal memory page size. Currently set to 4 KiBytes. */
	public static final int MIN_PAGE_SIZE = 4 * 1024;
	
	// --------------------------------------------------------------------------------------------
	
	private final Object lock = new Object();	 	// The lock used on the shared structures.
	
	private final ArrayDeque<byte[]> freeSegments;	// the free memory segments
	
	private final long roundingMask;		// mask used to round down sizes to multiples of the page size
	
	private final int pageSize;				// the page size, in bytes
	
	private final int pageSizeBits;			// the number of bits that the power-of-two page size corresponds to
	
	private final int totalNumPages;		// The initial total size, for verification.
	
	private int numPagesToBeAllocated;		// pages remaining to be allocated by the memory manager
	
	private boolean isShutDown;				// flag whether the close() has already been invoked.
	

	
	public DynamicMemoryManager(long memorySize) {
		this(memorySize, DEFAULT_PAGE_SIZE);
	}
	
	public DynamicMemoryManager(long memorySize, boolean lazyAllocation) {
		this(memorySize, DEFAULT_PAGE_SIZE, lazyAllocation);
	}
	
	public DynamicMemoryManager(long memorySize, int pageSize) {
		this(memorySize, pageSize, false);
	}
	
	public DynamicMemoryManager(long memorySize, int pageSize, boolean lazyAllocation) {
		// some sanity checks
		if (memorySize < 0) {
			throw new IllegalArgumentException("Memory size cannot be negative.");
		}
		if (pageSize < MIN_PAGE_SIZE) {
			throw new IllegalArgumentException("The requested page size (" + pageSize + " bytes) is below the minimal page size of " + MIN_PAGE_SIZE + " bytes.");
		}
		if (!MathUtils.isPowerOf2(pageSize)) {
			throw new IllegalArgumentException("The given page size is not a power of two.");
		}
		
		this.pageSize = pageSize;
		this.roundingMask = ~((long) (pageSize - 1));
		this.pageSizeBits = MathUtils.log2strict(pageSize);
		this.totalNumPages = getNumPages(memorySize);
		
		this.freeSegments = new ArrayDeque<byte[]>(totalNumPages);
		
		if (lazyAllocation) {
			this.numPagesToBeAllocated = totalNumPages;
		} else {
			for (int i = totalNumPages; i > 0; --i) {
				freeSegments.add(new byte[pageSize]);
			}
		}
	}
	
	
	public long getTotalMemorySize() {
		return ((long) totalNumPages) * pageSize;
	}
	
	public int getPageSize() {
		return pageSize;
	}
	
	public MemoryAllocator createMemoryAllocator(DynamicMemoryConsumer consumer, AllocationCategory category) {
		try {
			return createMemoryAllocator(consumer, category, 0);
		}
		catch (MemoryAllocationException e) {
			// this should never happen, since we request no minimal amount of memory. forward to be safe...
			throw new RuntimeException("Failed to create memory allocator that does not reserve memory.", e);
		}
	}
	
	public MemoryAllocator createMemoryAllocator(DynamicMemoryConsumer consumer, AllocationCategory category, int minimumNumberOfPages) throws MemoryAllocationException {
		return null;
	}
	
	public void releaseAll() {
		synchronized (lock) {
			
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	private final int getNumPages(long numBytes) {
		if (numBytes < 0) {
			throw new IllegalArgumentException("The number of bytes to allocate must not be negative.");
		}
		
		final long numPages = numBytes >>> this.pageSizeBits;
		if (numPages <= Integer.MAX_VALUE) {
			return (int) numPages;
		} else {
			throw new IllegalArgumentException("The given number of bytes correstponds to more than MAX_INT pages.");
		}
	}
}
