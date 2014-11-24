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

package org.apache.flink.runtime.operators.sort;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOMetrics;
import org.apache.flink.runtime.memorymanager.AllocationCategory;
import org.apache.flink.runtime.memorymanager.DynamicMemoryConsumer;
import org.apache.flink.runtime.memorymanager.DynamicMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryAllocationException;
import org.apache.flink.runtime.memorymanager.MemoryAllocator;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class AdaptiveExternalSorter<T> implements DynamicMemoryConsumer {
	
	/** Logger */
	private static final Logger LOG = LoggerFactory.getLogger(AdaptiveExternalSorter.class);
	
	/** The minimum number of segments that are required for the sort to operate */
	private static final int MIN_BUFFERS = 10;
	
	/** The memory allocation category : category operator level */
	private static final AllocationCategory DEFAULT_MEM_CATEGORY = AllocationCategory.OPERATOR;
	
	
	private static final int STATUS_NONE = 0;
	private static final int STATUS_SPILL_REQUESTED = 1;
	private static final int STATUS_CLOSED = 2;
	
	// --------------------------------------------------------------------------------------------
	
	private final MutableObjectIterator<T> input;
	
	private final TypeSerializer<T> serializer;
	
	private final TypeComparator<T> comparator;
	
	private final MemoryAllocator memAllocator;
	
	private final IOMetrics ioReporter;
	
	private final boolean multithreaded = false;
	
	private volatile int status;
	
	private boolean useMutableObjectMode;
	
	// --------------------------------------------------------------------------------------------
	
	public AdaptiveExternalSorter(MutableObjectIterator<T> input, DynamicMemoryManager memManager,
			boolean multithreaded)
		throws MemoryAllocationException
	{
		Preconditions.checkNotNull(input);
		Preconditions.checkNotNull(memManager);
		
		this.input = input;
		this.ioReporter = null;
		this.serializer = null;
		this.comparator = null;
		
		this.memAllocator = memManager.createMemoryAllocator(this, DEFAULT_MEM_CATEGORY, MIN_BUFFERS);
	}
	
	
	public void setUseMutableObjectMode(boolean useMutableObjectMode) {
		this.useMutableObjectMode = useMutableObjectMode;
	}
	
	public boolean getUseMutableObjectMode() {
		return useMutableObjectMode;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Main Work Methods
	// --------------------------------------------------------------------------------------------
	
	public void readAndSort() throws IOException {
		
		final MutableObjectIterator<T> input = this.input;
		final TypeSerializer<T> serializer = this.serializer;
		final boolean mutableObjects = this.useMutableObjectMode;
		
		NormalizedKeySorter<T> sorter = (NormalizedKeySorter<T>) new Object();
		
		T next = serializer.createInstance();
		
		int currentStatus;
		while ((currentStatus = this.status) != STATUS_CLOSED) {
			try {
				next = input.next(next);
				if (sorter.write(next)) {
				
				
			}
			catch (ReaderIterator.ReaderInterruptedException e) {
				// interrupted the reader. means that the status changed
			}
			
			next = mutableObjects ? next : serializer.createInstance();
		}
	}
	
	public void close() throws IOException {
		
	}
	
	// --------------------------------------------------------------------------------------------
	//  Dynamic Memory Behavior
	// --------------------------------------------------------------------------------------------

	@Override
	public void releaseMemory(int numPages) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void notifyMoreMemoryAvailable(int numPages) {
		// TODO Auto-generated method stub
	}
}
