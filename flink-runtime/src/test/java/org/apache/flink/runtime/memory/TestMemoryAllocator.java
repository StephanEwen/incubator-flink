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

package org.apache.flink.runtime.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memorymanager.MemoryAllocator;

public class TestMemoryAllocator implements MemoryAllocator {
	
	private final Object lock = new Object();
	
	private final Set<byte[]> allSegments;
	
	private final ArrayList<byte[]> availableSegments;
	
	private final int segmentSize;
	
	private boolean closed;
	
	
	public TestMemoryAllocator(int numMemorySegments, int segmentSize) {
		this.allSegments = new HashSet<byte[]>(numMemorySegments);
		this.availableSegments = new ArrayList<byte[]>(numMemorySegments);
		this.segmentSize = segmentSize;
		
		for (int i = 0; i < numMemorySegments; i++) {
			byte[] buffer = new byte[segmentSize];
			this.allSegments.add(buffer);
			this.availableSegments.add(buffer);
		}
	}
	
	
	@Override
	public int getMemorySegmentSize() {
		checkClosed();
		return segmentSize;
	}

	@Override
	public int getGuaranteedNumberOfSegments() {
		checkClosed();
		return allSegments.size();
	}

	@Override
	public MemorySegment nextSegment() {
		synchronized (lock) {
			checkClosed();
			
			if (availableSegments.isEmpty()) {
				return null;
			} else {
				return new TestMemorySegment(availableSegments.remove(availableSegments.size()-1));
			}
		}
	}

	@Override
	public void releaseSegment(MemorySegment segment) {
		if (!(segment instanceof TestMemorySegment)) {
			throw new IllegalArgumentException("MemorySegment did not come from this memory allocator.");
		}
		
		final TestMemorySegment seg = (TestMemorySegment) segment;
		if (!seg.assigned) {
			checkClosed();
			return;
		}
		
		synchronized (lock) {
			checkClosed();
			
			byte[] buffer = seg.dispose();
			
			if (!allSegments.contains(buffer)) {
				throw new IllegalArgumentException("MemorySegment did not come from this memory allocator.");
			}
			availableSegments.add(buffer);
		}
	}

	@Override
	public void releaseSegments(Collection<MemorySegment> segments) {
		synchronized (lock) {
			checkClosed();
			
			for (MemorySegment segment : segments) {
				
				if (!(segment instanceof TestMemorySegment)) {
					throw new IllegalArgumentException("MemorySegment did not come from this memory allocator.");
				}
				
				final TestMemorySegment seg = (TestMemorySegment) segment;
				if (!seg.assigned) {
					continue;
				}
				
				byte[] buffer = seg.dispose();
				
				if (!allSegments.contains(buffer)) {
					throw new IllegalArgumentException("MemorySegment did not come from this memory allocator.");
				}
				
				availableSegments.add(buffer);
			}
		}
	}
	
	@Override
	public void returnSegment(MemorySegment segment) {
		releaseSegment(segment);
	}


	@Override
	public void returnSegments(Collection<MemorySegment> segments) {
		releaseSegments(segments);
	}


	@Override
	public void returnAndReleaseSegments(Collection<MemorySegment> segments, int numToReturnOnly) {
		releaseSegments(segments);
	}

	@Override
	public void closeAndReleaseAll() {
		synchronized (lock) {
			closed = true;
			allSegments.clear();
			availableSegments.clear();
		}
	}

	@Override
	public void failOwner(Throwable cause) {}
	
	
	private void checkClosed() {
		if (closed) {
			throw new IllegalStateException("The memory allocator is closed.");
		}
	}
	
	public boolean allSegmentsAvailable() {
		return allSegments.size() == availableSegments.size();
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class TestMemorySegment extends MemorySegment {

		private boolean assigned = true;
		
		public TestMemorySegment(byte[] memory) {
			super(memory);
		}
		
		byte[] dispose() {
			byte[] buffer = this.memory;
			
			assigned = false;
			free();
			return buffer;
		}
	}
}
