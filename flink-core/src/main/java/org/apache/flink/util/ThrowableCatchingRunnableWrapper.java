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

package org.apache.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * A util class that helps wrap the runnable into {@link ThrowableCatchingRunnable}.
 */
public class ThrowableCatchingRunnableWrapper {
	private static final Logger LOG = LoggerFactory.getLogger(ThrowableCatchingRunnableWrapper.class);
	private final Logger logger;
	private final Consumer<Throwable> exceptionConsumer;

	public ThrowableCatchingRunnableWrapper(Consumer<Throwable> exceptionConsumer) {
		this(exceptionConsumer, null);
	}

	public ThrowableCatchingRunnableWrapper(Consumer<Throwable> exceptionConsumer, Logger logger) {
		this.logger = logger == null ? LOG : logger;
		this.exceptionConsumer = exceptionConsumer;
	}

	public ThrowableCatchingRunnable wrap(Runnable runnable) {
		return new ThrowableCatchingRunnable(exceptionConsumer, logger, runnable);
	}

	/**
	 * This class is needed to make sure all the exceptions are properly propagated to the
	 * SourceReader. Because we are using executors, the UncaughtExceptionHandler is not
	 * usable.
	 */
	public static class ThrowableCatchingRunnable implements Runnable {
		private static final Logger LOG = LoggerFactory.getLogger(ThrowableCatchingRunnableWrapper.class);
		private final Logger logger;
		private final Consumer<Throwable> exceptionConsumer;
		private final Runnable runnable;

		public ThrowableCatchingRunnable(
			Consumer<Throwable> exceptionConsumer,
			Logger logger,
			Runnable runnable) {
			this.logger = logger == null ? LOG : logger;
			this.exceptionConsumer = exceptionConsumer;
			this.runnable = runnable;
		}

		@Override
		public void run() {
			try {
				runnable.run();
			} catch (Throwable t) {
				logger.error("Received uncaught exception.", t);
				exceptionConsumer.accept(t);
			}
		}
	}
}
