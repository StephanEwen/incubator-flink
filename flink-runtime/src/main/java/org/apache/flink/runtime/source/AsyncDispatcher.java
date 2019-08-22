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

package org.apache.flink.runtime.source;

import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A dispatcher that makes calls in a dedicated thread, and forwards results to another executor.
 *
 * <p>This component can be used to have the calls on a specific component in a separate thread,
 * like in the following example:
 *
 * <pre>
 * {@code
 * private final SplitEnumerator<T> enumerator;
 *
 * public void triggerSnapshot() {
 *     dispatcher.callAsync(enumerator::snapshotState, this::handleSnapshotResponse);
 * }
 *
 * private void handleSnapshotResponse(@Nullable CheckpointT checkpoint, @Nullable Throwable failure) {
 *     if (failure == null) {
 *         // handle checkpoint failure
 *         return;
 *     }
 *
 *     // handle checkpoint complete
 * }
 * }
 * </pre>
 */
public final class AsyncDispatcher implements AutoCloseable {

	private final ExecutorService dispatcher;

	private final Executor callbackExecutor;

	public AsyncDispatcher(ExecutorService dispatcher, Executor callbackExecutor) {
		this.dispatcher = dispatcher;
		this.callbackExecutor = callbackExecutor;
	}

	// -------- dispatcher --------

	public <T> void callAsync(Callable<T> action, BiConsumer<T, Throwable> resultHandler) {
		dispatcher.execute(() -> {
			try {
				final T result = action.call();
				callbackExecutor.execute(() -> resultHandler.accept(result, null));
			}
			catch (Throwable t) {
				callbackExecutor.execute(() -> resultHandler.accept(null, t));
			}
		});
	}

	public void runAsync(Runnable action, Consumer<Throwable> resultHandler) {
		callAsync(runnableToCallable(action), consumerToBiConsumer(resultHandler));
	}

	@Override
	public void close() throws Exception {
		dispatcher.shutdownNow();
	}

	// -------- utils --------

	private static Callable<Void> runnableToCallable(Runnable runnable) {
		return () -> {
			runnable.run();
			return null;
		};
	}

	private static <T> BiConsumer<Void, T> consumerToBiConsumer(Consumer<T> consumer) {
		return (Void ignored, T t) -> consumer.accept(t);
	}

	// -------- factories --------

	/**
	 * Creates an AsyncDispatcher that has a dedicated thread for all calls and uses the
	 * given executor for all callbacks.
	 */
	public static AsyncDispatcher withDedicatedThread(
			Executor callbackExecutor,
			String threadName,
			ClassLoader contextClassLoader) {

		final DispatcherThreadFactory threadFactory = new DispatcherThreadFactory(
			Thread.currentThread().getThreadGroup(),
			threadName,
			contextClassLoader);

		final ExecutorService dispatcher = Executors.newSingleThreadExecutor(threadFactory);

		return new AsyncDispatcher(dispatcher, callbackExecutor);
	}

	/**
	 * Creates an AsyncDispatcher that synchronously calls the methods and the callbacks.
	 *
	 * <p>This mode is useful for testing components by forcing synchronous execution.
	 */
	public static AsyncDispatcher withSynchronousExecution() {
		final ExecutorService directExec = org.apache.flink.runtime.concurrent.Executors.newDirectExecutorService();
		return new AsyncDispatcher(directExec, directExec);
	}
}
