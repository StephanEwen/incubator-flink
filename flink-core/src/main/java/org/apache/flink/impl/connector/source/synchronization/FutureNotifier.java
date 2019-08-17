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

package org.apache.flink.impl.connector.source.synchronization;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A class facilitating the asynchronous communication among threads.
 */
public class FutureNotifier {
	private static final Object OBJ = new Object();
	/** A future reference. */
	private final AtomicReference<CompletableFuture<Object>> futureRef;

	public FutureNotifier() {
		this.futureRef = new AtomicReference<>(null);
	}

	/**
	 * Get the future out of this notifier. The future will be completed when someone invokes
	 * {@link #notifyComplete()}. If there is already an uncompleted future,
	 * that existing future will be returned instead of a new one.
	 *
	 * @return a future that will be completed.
	 */
	public CompletableFuture<Object> future() {
		futureRef.compareAndSet(null, new CompletableFuture<>());
		return futureRef.get();
	}

	/**
	 * Complete the future if there is one. This will release the thread that is waiting for data.
	 */
	public void notifyComplete() {
		CompletableFuture<Object> future = futureRef.get();
		// If there are multiple threads trying to complete the future, only the first one succeeds.
		if (future != null && future.complete(OBJ)) {
			futureRef.compareAndSet(future, null);
		}
	}
}
