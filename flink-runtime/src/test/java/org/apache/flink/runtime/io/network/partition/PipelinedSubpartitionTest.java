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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestInfiniteBufferProvider;
import org.apache.flink.runtime.io.network.util.TestNotificationListener;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.apache.flink.runtime.io.network.util.TestSubpartitionProducer;

import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.getMockBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PipelinedSubpartitionTest extends SubpartitionTestBase {

	/** Executor service for concurrent produce/consume tests */
	private final static ExecutorService executorService = Executors.newCachedThreadPool();

	@AfterClass
	public static void shutdownExecutorService() throws Exception {
		executorService.shutdownNow();
	}

	@Override
	PipelinedSubpartition createSubpartition() {
		final ResultPartition parent = mock(ResultPartition.class);

		return new PipelinedSubpartition(0, parent, 0);
	}

	@Test
	public void testRegisterListener() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		final TestNotificationListener listener = new TestNotificationListener();

		// Register a listener
		assertTrue(subpartition.registerListener(listener));

		// Try to register another listener
		try {
			subpartition.registerListener(listener);

			fail("Did not throw expected exception after duplicate listener registration.");
		}
		catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testListenerNotification() throws Exception {
		final TestNotificationListener listener = new TestNotificationListener();
		assertEquals(0, listener.getNumberOfNotifications());

		{
			final PipelinedSubpartition subpartition = createSubpartition();

			// Register a listener
			assertTrue(subpartition.registerListener(listener));

			// Notify on add and remove listener
			subpartition.add(mock(Buffer.class), false);
			assertEquals(1, listener.getNumberOfNotifications());

			// No notification, should have removed listener after first notification
			subpartition.add(mock(Buffer.class), false);
			assertEquals(1, listener.getNumberOfNotifications());
		}

		{
			final PipelinedSubpartition subpartition = createSubpartition();

			// Register a listener
			assertTrue(subpartition.registerListener(listener));

			// Notify on finish
			subpartition.finish();
			assertEquals(2, listener.getNumberOfNotifications());
		}

		{
			final PipelinedSubpartition subpartition = createSubpartition();

			// Register a listener
			assertTrue(subpartition.registerListener(listener));

			// Notify on release
			subpartition.release();
			assertEquals(3, listener.getNumberOfNotifications());
		}
	}

	@Test
	public void testIllegalReadViewRequest() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		// Successful request
		assertNotNull(subpartition.createReadView(null));

		try {
			subpartition.createReadView(null);

			fail("Did not throw expected exception after duplicate read view request.");
		}
		catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testBasicPipelinedProduceConsumeLogic() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		TestNotificationListener listener = new TestNotificationListener();

		ResultSubpartitionView view = subpartition.createReadView(null);

		// Empty => should return null
		assertNull(view.getNextBuffer());

		// Register listener for notifications
		assertTrue(view.registerListener(listener));

		assertEquals(0, listener.getNumberOfNotifications());

		// Add data to the queue...
		subpartition.add(createBuffer(), false);

		// ...should have resulted in a notification
		assertEquals(1, listener.getNumberOfNotifications());

		// ...and one available result
		assertNotNull(view.getNextBuffer());
		assertNull(view.getNextBuffer());

		// Add data to the queue...
		subpartition.add(createBuffer(), false);
		// ...don't allow to subscribe, if data is available
		assertFalse(view.registerListener(listener));

		assertEquals(1, listener.getNumberOfNotifications());
	}

	@Test
	public void testConcurrentFastProduceAndFastConsume() throws Exception {
		testProduceConsume(false, false);
	}

	@Test
	public void testConcurrentFastProduceAndSlowConsume() throws Exception {
		testProduceConsume(false, true);
	}

	@Test
	public void testConcurrentSlowProduceAndFastConsume() throws Exception {
		testProduceConsume(true, false);
	}

	@Test
	public void testConcurrentSlowProduceAndSlowConsume() throws Exception {
		testProduceConsume(true, true);
	}

	/**
	 * Test that the size bound is respected for add calls.
	 */
	@Test
	public void testBoundedSubPartitionWaitingInAdd() throws Exception {
		Thread producer = null;
		try {
			final int size = ThreadLocalRandom.current().nextInt(10) + 1;

			ResultPartition partition = mock(ResultPartition.class);
			final PipelinedSubpartition subpartition = new PipelinedSubpartition(0, partition, size);
			final CountDownLatch latch = new CountDownLatch(1);
			final AtomicReference<Throwable> error = new AtomicReference<>();

			producer = new Thread() {
				@Override
				public void run() {
					try {
						// Add one more than the bounded size.
						for (int i = 0; i < size + 1; i++) {
							assertTrue(subpartition.add(getMockBuffer(), false));
						}
						latch.countDown();
					} catch (Throwable e) {
						error.set(e);
					}
				}
			};

			producer.start();

			long deadline = System.currentTimeMillis() + 30 * 1_000;
			while (System.currentTimeMillis() <= deadline && producer.getState() != Thread.State.WAITING) {
				// Thread should be waiting for buffers. This is very much white
				// box testing the behaviour, but what else can we do here?
				Thread.sleep(1);
			}

			assertEquals(Thread.State.WAITING, producer.getState());
			assertEquals(subpartition.getCurrentSize(), size);

			PipelinedSubpartitionView read = subpartition.createReadView(new TestInfiniteBufferProvider());

			// Read all buffers
			for (int i = 0; i < size; i++) {
				assertNotNull(read.getNextBuffer());
			}

			latch.await();
			assertNotNull(read.getNextBuffer());

			assertEquals(0, subpartition.getCurrentSize());

			if (error.get() != null) {
				error.get().printStackTrace();
				fail("Producer failed: " + error.get().getMessage());
			}
		} finally {
			if (producer != null) {
				// Make sure the producer thread terminates
				producer.interrupt();
				producer.join();
			}
		}
	}

	/**
	 * Test that the size bound is respected for finish calls.
	 */
	@Test
	public void testBoundedSubPartitionWaitingInFinish() throws Exception {
		final int size = ThreadLocalRandom.current().nextInt(10) + 1;

		ResultPartition partition = mock(ResultPartition.class);
		final PipelinedSubpartition subpartition = new PipelinedSubpartition(0, partition, size);

		// Add as much as we can.
		for (int i = 0; i < size; i++) {
			assertTrue(subpartition.add(getMockBuffer(), false));
		}

		// this should not block, even though the queue is already at capacity
		subpartition.finish();

		PipelinedSubpartitionView read = subpartition.createReadView(new TestInfiniteBufferProvider());

		// Read all buffers
		for (int i = 0; i < size; i++) {
			assertNotNull(read.getNextBuffer());
		}

		Buffer eopBuffer = read.getNextBuffer();
		assertNotNull(eopBuffer);
		assertFalse(eopBuffer.isBuffer());
		assertEquals(EndOfPartitionEvent.class, EventSerializer.fromBuffer(eopBuffer, Thread.currentThread().getContextClassLoader()).getClass());

		assertEquals(0, subpartition.unsynchronizedGetCurrentSize());
	}

	/**
	 * Test that a waiting add operation reacts to a cancellation.
	 */
	@Test
	public void testBoundedSubPartitionReleasedWhileWaitingInAdd() throws Exception {
		Thread producer = null;
		try {
			ResultPartition partition = mock(ResultPartition.class);
			final PipelinedSubpartition subpartition = new PipelinedSubpartition(0, partition, 1);
			final AtomicReference<Throwable> error = new AtomicReference<>();

			producer = new Thread() {
				@Override
				public void run() {
					try {
						assertTrue(subpartition.add(getMockBuffer(), false));
						assertFalse(subpartition.add(getMockBuffer(), false));
					} catch (Throwable e) {
						error.set(e);
					}
				}
			};

			producer.start();

			long deadline = System.currentTimeMillis() + 30 * 1_000;
			while (System.currentTimeMillis() <= deadline && producer.getState() != Thread.State.WAITING) {
				// Thread should be waiting for buffers. This is very much white
				// box testing the behaviour, but what else can we do here?
				Thread.sleep(1);
			}

			assertEquals(Thread.State.WAITING, producer.getState());
			assertEquals(subpartition.getCurrentSize(), 1);

			// Release it
			subpartition.release();

			producer.join();

			if (error.get() != null) {
				error.get().printStackTrace();
				fail("Producer failed: " + error.get().getMessage());
			}
		} finally {
			if (producer != null) {
				// Make sure the producer thread terminates
				producer.interrupt();
				producer.join();
			}
		}
	}


	private void testProduceConsume(boolean isSlowProducer, boolean isSlowConsumer) throws Exception {
		// Config
		final int producerBufferPoolSize = 8;
		final int producerNumberOfBuffersToProduce = 128;

		// Producer behaviour
		final TestProducerSource producerSource = new TestProducerSource() {

			private BufferProvider bufferProvider = new TestPooledBufferProvider(producerBufferPoolSize);

			private int numberOfBuffers;

			@Override
			public BufferOrEvent getNextBufferOrEvent() throws Exception {

				if (numberOfBuffers == producerNumberOfBuffersToProduce) {
					return null;
				}

				final Buffer buffer = bufferProvider.requestBufferBlocking();

				final MemorySegment segment = buffer.getMemorySegment();

				int next = numberOfBuffers * (segment.size() / 4);

				for (int i = 0; i < segment.size(); i += 4) {
					segment.putInt(i, next);

					next++;
				}

				numberOfBuffers++;

				return new BufferOrEvent(buffer, 0);
			}
		};

		// Consumer behaviour
		final TestConsumerCallback consumerCallback = new TestConsumerCallback() {

			private int numberOfBuffers;

			@Override
			public void onBuffer(Buffer buffer) {
				final MemorySegment segment = buffer.getMemorySegment();

				int expected = numberOfBuffers * (segment.size() / 4);

				for (int i = 0; i < segment.size(); i += 4) {
					assertEquals(expected, segment.getInt(i));

					expected++;
				}

				numberOfBuffers++;

				buffer.recycle();
			}

			@Override
			public void onEvent(AbstractEvent event) {
				// Nothing to do in this test
			}
		};

		final PipelinedSubpartition subpartition = createSubpartition();

		final PipelinedSubpartitionView view = subpartition.createReadView(null);

		Future<Boolean> producer = executorService.submit(
				new TestSubpartitionProducer(subpartition, isSlowProducer, producerSource));

		Future<Boolean> consumer = executorService.submit(
				new TestSubpartitionConsumer(view, isSlowConsumer, consumerCallback));

		// Wait for producer and consumer to finish
		producer.get();
		consumer.get();
	}
}
