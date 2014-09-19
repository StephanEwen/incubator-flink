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

import java.io.EOFException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.TestMemoryAllocator;
import org.apache.flink.runtime.memorymanager.AllocationCategory;
import org.apache.flink.runtime.memorymanager.DynamicMemoryConsumer;
import org.apache.flink.runtime.memorymanager.DynamicMemoryManager;
import org.apache.flink.runtime.operators.testutils.PairGenerator;
import org.apache.flink.runtime.operators.testutils.PairGenerator.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SpillingBufferMemoryAdaptiveTest {
	
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
	public void testInMemory() {
		final int NUM_PAGES = 10;
		final int NUM_RECORDS = 10000;
		final int PAGE_SIZE = 32 * 1024;
		
		try {
			PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			
			SpillingBufferMemoryAdaptive buffer = new SpillingBufferMemoryAdaptive(
					createMemoryManager(NUM_PAGES, PAGE_SIZE), ioManager, AllocationCategory.OPERATOR);
			
			Pair forComparison = new Pair();
			Pair pair = new Pair();
			
			// write the records
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(pair);
				pair.write(buffer);
			}

			assertTrue(buffer.isInMemory());
			
			// reset and read incomplete
			DataInputView in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
		
			// reset (twice) and read complete
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS / 3; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// reset and read
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// read too much
			try {
				pair.read(in);
				fail("should throw an EOF Exception");
			}
			catch (EOFException e) {}
			
			buffer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSpillingDuringWrite() {
		final int NUM_PAGES = 5;
		final int NUM_RECORDS = 50000;
		final int PAGE_SIZE = 32 * 1024;
		
		try {
			PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			
			SpillingBufferMemoryAdaptive buffer = new SpillingBufferMemoryAdaptive(
					createMemoryManager(NUM_PAGES, PAGE_SIZE), ioManager, AllocationCategory.OPERATOR);
			
			Pair forComparison = new Pair();
			Pair pair = new Pair();
			
			// write the records
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(pair);
				pair.write(buffer);
			}

			assertFalse(buffer.isInMemory());
			
			// reset and read incomplete
			DataInputView in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
		
			// reset (twice) and read complete
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS / 3; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// reset and read
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// read too much
			try {
				pair.read(in);
				fail("should throw an EOF Exception");
			}
			catch (EOFException e) {}
			
			// release memory and make sure that the temp file is deleted
			buffer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSpillingAtEndOfWrite() {
		final int NUM_PAGES = 25;
		final int NUM_RECORDS = 50000;
		final int PAGE_SIZE = 32 * 1024;
		
		try {
			PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			
			SpillingBufferMemoryAdaptive buffer = new SpillingBufferMemoryAdaptive(
					createMemoryManager(NUM_PAGES, PAGE_SIZE), ioManager, AllocationCategory.OPERATOR);
			
			Pair forComparison = new Pair();
			Pair pair = new Pair();
			
			// write the records
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(pair);
				pair.write(buffer);
			}

			// must still be in memory
			assertTrue(buffer.isInMemory());
			
			// manually trigger a spill
			buffer.releaseMemory(10);
			
			// must be on disk
			assertFalse(buffer.isInMemory());
			
			// reset and read incomplete
			DataInputView in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
		
			// reset (twice) and read complete
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS / 3; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// reset and read
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// read too much
			try {
				pair.read(in);
				fail("should throw an EOF Exception");
			}
			catch (EOFException e) {}
			
			// release memory and make sure that the temp file is deleted
			buffer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSpillingDuringReadBuffers() {
		final int NUM_PAGES = 25;
		final int NUM_RECORDS = 50000;
		final int PAGE_SIZE = 32 * 1024;
		
		try {
			PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			
			SpillingBufferMemoryAdaptive buffer = new SpillingBufferMemoryAdaptive(
					createMemoryManager(NUM_PAGES, PAGE_SIZE), ioManager, AllocationCategory.OPERATOR);
			
			Pair forComparison = new Pair();
			Pair pair = new Pair();
			
			// write the records
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(pair);
				pair.write(buffer);
			}
			
			// reset and read incomplete
			DataInputView in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS * 2 / 3; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// must still be in memory
			assertTrue(buffer.isInMemory());
			// externally trigger a spill
			buffer.releaseMemory(10);
			// must be on disk
			assertFalse(buffer.isInMemory());
			
			// continue the read
			for (int i = NUM_RECORDS * 2 / 3; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
		
			// reset (twice) and read complete
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS / 3; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// reset and read
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// read too much
			try {
				pair.read(in);
				fail("should throw an EOF Exception");
			}
			catch (EOFException e) {}
			
			// release memory and make sure that the temp file is deleted
			buffer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSpillingBetweenReads() {
		final int NUM_PAGES = 25;
		final int NUM_RECORDS = 50000;
		final int PAGE_SIZE = 32 * 1024;
		
		try {
			PairGenerator generator = new PairGenerator(764392569385L ^ System.currentTimeMillis(), 100000, 10);
			
			SpillingBufferMemoryAdaptive buffer = new SpillingBufferMemoryAdaptive(
					createMemoryManager(NUM_PAGES, PAGE_SIZE), ioManager, AllocationCategory.OPERATOR);
			
			Pair forComparison = new Pair();
			Pair pair = new Pair();
			
			// write the records
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(pair);
				pair.write(buffer);
			}
			
			// must be in memory still
			assertTrue(buffer.isInMemory());
			
			// reset and read incomplete
			DataInputView in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// must still be in memory
			assertTrue(buffer.isInMemory());
			// manually trigger a spill
			buffer.releaseMemory(10);
			// must still be on disk
			assertFalse(buffer.isInMemory());
			
			
			// reset (twice) and read complete
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS / 3; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// reset and read
			buffer.rewindAndGetInput();
			in = buffer.rewindAndGetInput();
			generator.reset();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				generator.next(forComparison);
				pair.read(in);
				
				assertEquals(forComparison, pair);
			}
			
			// read too much
			try {
				pair.read(in);
				fail("should throw an EOF Exception");
			}
			catch (EOFException e) {}
			
			// release memory and make sure that the temp file is deleted
			buffer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static DynamicMemoryManager createMemoryManager(int numPages, int pageSize) throws Exception {
		TestMemoryAllocator alloc = new TestMemoryAllocator(numPages, pageSize);
		
		DynamicMemoryManager mm = mock(DynamicMemoryManager.class);
		when(mm.getPageSize()).thenReturn(pageSize);
		when(mm.createMemoryAllocator(any(DynamicMemoryConsumer.class), any(AllocationCategory.class), anyInt())).thenReturn(alloc);
		when(mm.createMemoryAllocator(any(DynamicMemoryConsumer.class), any(AllocationCategory.class))).thenReturn(alloc);
		
		return mm;
	}
}
