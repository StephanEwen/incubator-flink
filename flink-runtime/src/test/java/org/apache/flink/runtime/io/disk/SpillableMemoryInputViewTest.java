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

import static org.junit.Assert.*;

import java.io.EOFException;
import java.util.ArrayList;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.TestMemoryAllocator;
import org.apache.flink.runtime.memorymanager.SimpleCollectingOutputView;
import org.apache.flink.runtime.operators.testutils.PairGenerator;
import org.apache.flink.runtime.operators.testutils.PairGenerator.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SpillableMemoryInputViewTest {
	
	private IOManager ioManager;
	
	@Before
	public void setupIoManager() {
		ioManager = new IOManagerAsync();
	}
	
	@After
	public void shutdownIoManager() {
		ioManager.shutdown();
		ioManager = null;
	}

	@Test
	public void testInMemoryOneBufferOnly() {
		runTestsInMemory(1000, 1, ioManager);
	}
	
	@Test
	public void testInMemoryMultipleBuffers() {
		runTestsInMemory(10000, 5, ioManager);
	}
	
	@Test
	public void testSpillingOneBufferOnly() {
		runTestsWithSpilling(1000, 500, 1, 1, ioManager);
	}
	
	@Test
	public void testSpillingMultipleBuffers() {
		runTestsWithSpilling(20000, 3000, 10, 3, ioManager);
	}
	
	@Test
	public void testSpillingMultipleBuffersRetainOne() {
		runTestsWithSpilling(20000, 3000, 10, 1, ioManager);
	}
	
	@Test
	public void testSpillingAtTheEnd() {
		runTestsWithSpilling(20000, 20000, 10, 3, ioManager);
	}

	@Test
	public void testSpillingAtTheEndRetainOne() {
		runTestsWithSpilling(20000, 20000, 10, 1, ioManager);
	}
	
	public static void runTestsInMemory(int numRecords, int numPages, IOManager ioManager) {
		try {
			final int pageSize = 32 * 1024;
			
			TestMemoryAllocator alloc = new TestMemoryAllocator(numPages, pageSize);
			ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>();
			
			PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			final int segmentBytes;
			
			// write one segment full
			{
				SimpleCollectingOutputView outView = new SimpleCollectingOutputView(memory, alloc, alloc.getMemorySegmentSize());
				
				Pair writer = new Pair();
				for (int i = 0; i < numRecords; i++) {
					generator.next(writer);
					writer.write(outView);
				}
				
				segmentBytes = outView.getCurrentPositionInSegment();
				generator.reset();
			}
			
			// create the memory segment
			final SpillableMemoryInputView in = new SpillableMemoryInputView(memory, alloc, ioManager, 
					alloc.getMemorySegmentSize(), segmentBytes, 1);
			
			Pair forComparison = new Pair();
			Pair readPair = new Pair();
			
			// read for the first time, incompletely
			for (int i = 0; i < numRecords * 2 / 3; i++) {
				generator.next(forComparison);
				readPair.read(in);
				
				assertEquals(forComparison, readPair);
			}

			// reset and read incomplete
			in.reset();
			generator.reset();
			
			for (int i = 0; i < numRecords / 3; i++) {
				generator.next(forComparison);
				readPair.read(in);
				
				assertEquals(forComparison, readPair);
			}
		
			// reset and read complete
			in.reset();
			generator.reset();
			
			for (int i = 0; i < numRecords; i++) {
				generator.next(forComparison);
				readPair.read(in);
				
				assertEquals(forComparison, readPair);
			}
			
			// reset twice and read only one record
			in.reset();
			in.reset();
			readPair.read(in);
		
			// reset and read complete
			in.reset();
			generator.reset();
			
			for (int i = 0; i < numRecords; i++) {
				generator.next(forComparison);
				readPair.read(in);
				
				assertEquals(forComparison, readPair);
			}
			
			// read too much
			try {
				readPair.read(in);
				fail("should throw an EOF Exception");
			}
			catch (EOFException e) {}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public static void runTestsWithSpilling(int numRecords, int spillAfter, int numPages, int numRetained, IOManager ioManager) {
		assertTrue("invalid test", spillAfter <= numRecords);
		
		SpillableMemoryInputView in = null;
		
		try {
			final int pageSize = 32 * 1024;
			
			TestMemoryAllocator alloc = new TestMemoryAllocator(numPages, pageSize);
			ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>();
			
			PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			final int segmentBytes;
			
			// write one segment full
			{
				SimpleCollectingOutputView outView = new SimpleCollectingOutputView(memory, alloc, alloc.getMemorySegmentSize());
				
				Pair writer = new Pair();
				for (int i = 0; i < numRecords; i++) {
					generator.next(writer);
					writer.write(outView);
				}
				
				segmentBytes = outView.getCurrentPositionInSegment();
				generator.reset();
			}
			
			// create the memory segment
			in = new SpillableMemoryInputView(memory, alloc, ioManager, 
					alloc.getMemorySegmentSize(), segmentBytes, numRetained);
			
			Pair forComparison = new Pair();
			Pair readPair = new Pair();
			
			// read for the first time, incompletely
			for (int i = 0; i < spillAfter; i++) {
				generator.next(forComparison);
				readPair.read(in);
				
				assertEquals(forComparison, readPair);
			}

			// now spill !
			in.spill();
			
			assertFalse(in.isInMemory());
			
			// go on reading from where we were, to the end
			for (int i = spillAfter; i < numRecords; i++) {
				generator.next(forComparison);
				readPair.read(in);
				
				assertEquals(forComparison, readPair);
			}
			
			// read one more, incompletely
			in.reset();
			generator.reset();
			
			for (int i = 0; i < numRecords / 3; i++) {
				generator.next(forComparison);
				readPair.read(in);
				
				assertEquals(forComparison, readPair);
			}
			
			// reset twice and read only one record
			in.reset();
			in.reset();
			readPair.read(in);
		
			// reset and read complete
			in.reset();
			generator.reset();
			
			for (int i = 0; i < numRecords; i++) {
				generator.next(forComparison);
				readPair.read(in);
				
				assertEquals(forComparison, readPair);
			}
			
			// read too much
			try {
				readPair.read(in);
				fail("should throw an EOF Exception");
			}
			catch (EOFException e) {}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			try {
				in.close();
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
	}
}
