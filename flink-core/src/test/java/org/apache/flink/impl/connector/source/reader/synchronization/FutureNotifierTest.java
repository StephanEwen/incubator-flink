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

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for future notifier.
 */
public class FutureNotifierTest {

	@Test
	public void testCallFutureRepeatedly() {
		FutureNotifier futureNotifier = new FutureNotifier();
		assertSame("Two invocations should return the same future object.",
					 futureNotifier.future(), futureNotifier.future());
	}

	@Test
	public void testNotifyComplete() {
		FutureNotifier futureNotifier = new FutureNotifier();
		CompletableFuture<?> future = futureNotifier.future();
		assertFalse("Future should have not been completed yet.", future.isDone());
		futureNotifier.notifyComplete();
		assertTrue("Future should have been completed.", future.isDone());
	}
}
