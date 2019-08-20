/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.impl.connector.source.reader.synchronization;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link FutureCompletingBlockingQueue}.
 */
public class FutureCompletingBlockingQueueTest {

	@Test
	public void testPut() throws Exception {
		testAction(q -> {
			try {
				q.put(0);
			} catch (InterruptedException e) {
				fail("Unexpected InterruptedException.");
			}
		});
	}

	@Test
	public void testOffer() throws Exception {
		testAction(q -> q.offer(0));
	}

	@Test
	public void testOfferWithTimeout() throws Exception {
		testAction(q -> {
			try {
				q.offer(0, 1, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				fail("Unexpected InterruptedException.");
			}
		});
	}

	@Test
	public void testAdd() throws Exception {
		testAction(q -> q.add(0));
	}

	@Test
	public void testAddAll() throws Exception {
		List<Integer> values = Arrays.asList(1, 2, 3);
		testAction(q -> q.addAll(values));
	}

	private void testAction(Consumer<BlockingQueue<Integer>> action) throws Exception {
		FutureNotifier futureNotifier = new FutureNotifier();
		BlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(futureNotifier);
		CompletableFuture future = futureNotifier.future();
		assertFalse("Future should not have been completed yet.", future.isDone());
		action.accept(queue);
		assertTrue("Future should have been completed.", future.isDone());
	}
}
