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
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.TestMemoryAllocator;
import org.apache.flink.runtime.memorymanager.AllocationCategory;
import org.apache.flink.runtime.memorymanager.DynamicMemoryConsumer;
import org.apache.flink.runtime.memorymanager.DynamicMemoryManager;
import org.apache.flink.runtime.operators.testutils.PairGenerator;
import org.apache.flink.runtime.operators.testutils.PairGenerator.Pair;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SpillingBufferMemoryAdaptiveITCase {
	
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
	public void testSpillDuringWrite() {
		runWithAsynchronousSpillRequestDuringWrite(3000000, 11000, 1500, ioManager);
		runWithAsynchronousSpillRequestDuringWrite(3000000, 1000000, 1500, ioManager);
		runWithAsynchronousSpillRequestDuringWrite(3000000, 2999999, 1500, ioManager);
	}
	
	@Test
	public void testSpillDuringRead() {
		runWithAsynchronousSpillRequestDuringRead(3000000, 0, 1500, ioManager);
		runWithAsynchronousSpillRequestDuringRead(3000000, 10000, 1500, ioManager);
		runWithAsynchronousSpillRequestDuringRead(3000000, 2999999, 1500, ioManager);
	}
	
	private static DynamicMemoryManager createMemoryManager(int numPages, int pageSize) throws Exception {
		TestMemoryAllocator alloc = new TestMemoryAllocator(numPages, pageSize);
		
		DynamicMemoryManager mm = mock(DynamicMemoryManager.class);
		when(mm.getPageSize()).thenReturn(pageSize);
		when(mm.createMemoryAllocator(any(DynamicMemoryConsumer.class), any(AllocationCategory.class), anyInt())).thenReturn(alloc);
		when(mm.createMemoryAllocator(any(DynamicMemoryConsumer.class), any(AllocationCategory.class))).thenReturn(alloc);
		
		return mm;
	}
	
	private static void runWithAsynchronousSpillRequestDuringWrite(final int numRecords, final int spillAfter, int numPages, IOManager ioManager) {
		final int PAGE_SIZE = 32 * 1024;
		
		try {
			final DynamicMemoryManager memManager = createMemoryManager(numPages, PAGE_SIZE);
			final PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			
			final SpillingBufferMemoryAdaptive buffer = new SpillingBufferMemoryAdaptive(memManager, ioManager, AllocationCategory.OPERATOR);
			
			final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
			final AtomicBoolean spillTrigger = new AtomicBoolean();
			final Object lock = new Object();
			
			Runnable readerWriter = new Runnable() {
				@Override
				public void run() {
					try {
						final Pair forComparison = new Pair();
						final Pair pair = new Pair();
						
						// read for the first time, completely
						for (int i = 0; i < numRecords; i++) {
							generator.next(pair);
							pair.write(buffer);
							
							if (i == spillAfter) {
								synchronized (lock) {
									spillTrigger.set(true);
									lock.notifyAll();
								}
							}
						}
						
						while (buffer.isInMemory()) {
							CommonTestUtils.sleepUninterruptibly(10);
						}
						
						generator.reset();
						DataInputView in = buffer.rewindAndGetInput();

						// read and trigger the spill in the meantime
						for (int i = 0; i < numRecords; i++) {
							generator.next(forComparison);
							pair.read(in);
							
							assertEquals(forComparison, pair);
						}
					
						// reset and read complete
						in = buffer.rewindAndGetInput();
						generator.reset();
						
						for (int i = 0; i < numRecords; i++) {
							generator.next(forComparison);
							pair.read(in);
							
							assertEquals(forComparison, pair);
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
			Thread runner = new Thread(readerWriter, "Consumer");
			runner.start();
			
			synchronized (lock) {
				while (exception.get() == null && !spillTrigger.get()) {
					lock.wait();
				}
			}
			
			if (exception.get() == null) {
				// trigger the spill
				buffer.releaseMemory(numPages - 10);
				
				// wait for the reader to finish
				runner.join();
			}
			
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
	
	private static void runWithAsynchronousSpillRequestDuringRead(final int numRecords, final int spillAfter, int numPages, IOManager ioManager) {
		final int PAGE_SIZE = 32 * 1024;
		
		try {
			final DynamicMemoryManager memManager = createMemoryManager(numPages, PAGE_SIZE);
			final PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			
			final SpillingBufferMemoryAdaptive buffer = new SpillingBufferMemoryAdaptive(memManager, ioManager, AllocationCategory.OPERATOR);
			
			final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
			final AtomicBoolean spillTrigger = new AtomicBoolean();
			final Object lock = new Object();
			
			Runnable readerWriter = new Runnable() {
				@Override
				public void run() {
					try {
						final Pair forComparison = new Pair();
						final Pair pair = new Pair();
						
						// write
						for (int i = 0; i < numRecords; i++) {
							generator.next(pair);
							pair.write(buffer);
						}

						// read for the first time
						DataInputView in = buffer.rewindAndGetInput();
						generator.reset();
						
						for (int i = 0; i < numRecords; i++) {
							
							generator.next(forComparison);
							pair.read(in);
							
							assertEquals(forComparison, pair);
						}

						// read and trigger the spill in the meantime
						in = buffer.rewindAndGetInput();
						generator.reset();
						
						for (int i = 0; i < numRecords; i++) {
							
							if (i == spillAfter) {
								synchronized (lock) {
									spillTrigger.set(true);
									lock.notifyAll();
								}
							}
							
							generator.next(forComparison);
							pair.read(in);
							
							assertEquals(forComparison, pair);
						}
					
						// reset and read complete
						in = buffer.rewindAndGetInput();
						generator.reset();
						
						for (int i = 0; i < numRecords; i++) {
							generator.next(forComparison);
							pair.read(in);
							
							assertEquals(forComparison, pair);
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
			Thread runner = new Thread(readerWriter, "Consumer");
			runner.start();
			
			synchronized (lock) {
				while (exception.get() == null && !spillTrigger.get()) {
					lock.wait();
				}
			}
			
			if (exception.get() == null) {
				// trigger the spill
				buffer.releaseMemory(numPages - 10);
				
				// wait for the reader to finish
				runner.join();
			}
			
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
