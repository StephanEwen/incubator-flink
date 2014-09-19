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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.TestMemoryAllocator;
import org.apache.flink.runtime.memorymanager.SimpleCollectingOutputView;
import org.apache.flink.runtime.operators.testutils.PairGenerator;
import org.apache.flink.runtime.operators.testutils.PairGenerator.Pair;

public class SpillableMemoryInputViewITCase {

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
	public void testInMemoryMultipleBuffers() {
		SpillableMemoryInputViewTest.runTestsInMemory(3000000, 1500, ioManager);
	}
	
	@Test
	public void testSpillingMultipleBuffers() {
		SpillableMemoryInputViewTest.runTestsWithSpilling(3000000, 900000, 1500, 4, ioManager);
	}
	
	@Test
	public void testConcurrentSpillCalls() {
		runWithAsynchronousSpillRequest(3000000, 0, 1500, 4, ioManager);
		
		runWithAsynchronousSpillRequest(3000000, 100000, 1500, 4, ioManager);
		
		runWithAsynchronousSpillRequest(3000000, 1000000, 1500, 4, ioManager);
		
		runWithAsynchronousSpillRequest(3000000, 2999999, 1500, 4, ioManager);
	}
	
	private static void runWithAsynchronousSpillRequest(final int numRecords, final int spillAfter, int numPages, int numRetained,
			IOManager ioManager)
	{
		try {
			final int pageSize = 32 * 1024;
			
			TestMemoryAllocator alloc = new TestMemoryAllocator(numPages, pageSize);
			ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>();
			
			final PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
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
					alloc.getMemorySegmentSize(), segmentBytes, numRetained);
			
			final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
			final AtomicBoolean spillTrigger = new AtomicBoolean();
			final Object lock = new Object();
			
			Runnable reader = new Runnable() {
				@Override
				public void run() {
					try {
						final Pair forComparison = new Pair();
						final Pair readPair = new Pair();
						
						// read for the first time, completely
						for (int i = 0; i < numRecords; i++) {
							generator.next(forComparison);
							readPair.read(in);
							
							assertEquals(forComparison, readPair);
						}

						in.reset();
						generator.reset();

						// read and trigger the spill in the meantime
						for (int i = 0; i < numRecords; i++) {
							
							if (i == spillAfter) {
								synchronized (lock) {
									spillTrigger.set(true);
									lock.notifyAll();
								}
							}
							
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
					}
					catch (Throwable t) {
						// make sure the main thread proceeds
						exception.set(t);
						synchronized (lock) {
							lock.notifyAll();
						}
					}
				}
			};
			
			// start the reader and wait until we should issue the spill request
			Thread runner = new Thread(reader, "Consumer");
			runner.start();
			
			synchronized (lock) {
				while (exception.get() == null && !spillTrigger.get()) {
					lock.wait();
				}
			}
			
			// trigger the spill
			in.spill();
			
			// wait for the reader to finish
			runner.join();
			
			// check for an error
			if (exception.get() != null) {
				exception.get().printStackTrace();
				fail("Reader thread failed: " + exception.get().getMessage());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
