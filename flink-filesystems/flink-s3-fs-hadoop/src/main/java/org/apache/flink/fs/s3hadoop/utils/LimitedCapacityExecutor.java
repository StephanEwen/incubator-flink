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

package org.apache.flink.fs.s3hadoop.utils;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * <p>This class does not implement
 */
public class LimitedCapacityExecutor {

	/** The executor for the actual execution. */
	private final Executor delegate;

	/** The semaphore to track permits and block until permits are available. */
	private final Semaphore permits;

	public LimitedCapacityExecutor(Executor delegate, int numConcurrentExecutions) {
		checkArgument(numConcurrentExecutions > 0, "numConcurrentExecutions must be > 0");
		this.delegate = checkNotNull(delegate, "delegate");
		this.permits = new Semaphore(numConcurrentExecutions, true);
	}

	public void enqueue(Runnable command) throws IOException {
		try {
			permits.acquire();
		}


	}

	// ------------------------------------------------------------------------

	private static class SemaphoreReleasingRunnable implements Runnable {

		private final Runnable delegate;

		private final Semaphore toRelease;

		SemaphoreReleasingRunnable(Runnable delegate, Semaphore toRelease) {
			this.delegate = delegate;
			this.toRelease = toRelease;
		}

		@Override
		public void run() {
			try {
				delegate.run();
			}
			finally {
				toRelease.release();
			}
		}
	}
}
