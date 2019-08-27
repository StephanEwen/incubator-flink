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

package org.apache.flink.impl.connector.source.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * This class is used to coordinate between two components, where one component has an
 * executor following the mailbox model and the other component notifies it when needed.
 */
public class ExecutorNotifier {
	private static final Logger LOG = LoggerFactory.getLogger(ExecutorNotifier.class);
	private ScheduledExecutorService workerExecutor;
	private Executor executorToNotify;
	private Runnable onNotificationAction;

	public ExecutorNotifier(ScheduledExecutorService workerExecutor,
							Executor executorToNotify,
							Runnable onNotificationAction) {
		this.onNotificationAction = onNotificationAction;
		this.executorToNotify = executorToNotify;
		this.workerExecutor = workerExecutor;
	}

	/**
	 * Notify the {@link #executorToNotify} to execute the onNotificationAction.
	 */
	public void notifyReady() {
		executorToNotify.execute(onNotificationAction);
	}

	/**
	 * Call the given callable once. Notify the {@link #executorToNotify} to execute
	 * the onNotificationAction when the handler returns true.
	 *
	 * <p>It is important to make sure that the callable and handler does not modify
	 * any shared state. Otherwise there might be unexpected behavior. For example, the
	 * following code should be avoided.
	 *
	 * <pre>
	 * 	{
	 * 		final List<Integer> list = new ArrayList<>();
	 * 		// The onNotificationAction is to remove a number from list.
	 * 		ExecutorNotifier notifier = new ExecutorNotifier(workerExecutor, executorToNotify, () -> list.remove(0));
	 *
	 * 		// The callable adds an integer 1 to the list, while it works at the first glance,
	 * 		// A ConcurrentModificationException may be thrown if the callable is called while the
	 * 		// executorToNotify is running its onNotificationAction.
	 * 		notifier.notifyReadyAsync(() -> list.add(1), (ignoredValue, ignoredThrowable) -> true);
	 * 	}
	 * </pre>
	 *
	 * Instead, the above logic should be implemented in as:
	 * <pre>
	 *	{
	 *		// Modify the state in the handler.
	 *		notifier.notifyReadyAsync(() -> 1, (v, ignoredThrowable) -> list.add(v));
	 *	}
	 * </pre>
	 *
	 * @param callable the callable to invoke before notifying the executor.
	 * @param handler the handler to handle the result of the callable.
	 */
	public <T> void notifyReadyAsync(Callable<T> callable, BiFunction<T, Throwable, Boolean> handler) {
		workerExecutor.execute(() -> {
			try {
				T result = callable.call();
				executorToNotify.execute(() -> {
					if (handler.apply(result, null)) {
						executorToNotify.execute(onNotificationAction);
					}
				});
			} catch (Throwable t) {
				LOG.error("Unexpected exception {}", t);
				handler.apply(null, t);
			}
		});
	}

	/**
	 * Call the given callable periodically. Notify the {@link #executorToNotify} to
	 * execute the onNotificationAction when the handler returns true.
	 *
	 * <p>It is important to make sure that the callable and handler does not modify
	 * any shared state. Otherwise there might be unexpected behavior. For example, the
	 * following code should be avoided.
	 *
	 * <pre>
	 * 	{
	 * 		final List<Integer> list = new ArrayList<>();
	 * 		// The onNotificationAction is to remove a number from list.
	 * 		ExecutorNotifier notifier = new ExecutorNotifier(workerExecutor, executorToNotify, () -> list.remove(0));
	 *
	 * 		// The callable adds an integer 1 to the list, while it works at the first glance,
	 * 		// A ConcurrentModificationException may be thrown if the callable is called while the
	 * 		// executorToNotify is running its onNotificationAction.
	 * 		notifier.notifyReadyAsync(() -> list.add(1), (ignoredValue, ignoredThrowable) -> true, 0L, 100L);
	 * 	}
	 * </pre>
	 *
	 * Instead, the above logic should be implemented in as:
	 * <pre>
	 *	{
	 *		// Modify the state in the handler.
	 *		notifier.notifyReadyAsync(() -> 1, (v, ignoredThrowable) -> list.add(v), 0L, 100L);
	 *	}
	 * </pre>
	 *
	 * @param callable the callable to execute before notifying the executor to notify.
	 * @param handler the handler that handles the result from the callable.
	 * @param initialDelayMs the initial delay in ms before invoking the given callable.
	 * @param periodMs the interval in ms to invoke the callable.
	 */
	public <T> void notifyReadyAsync(Callable<T> callable,
									 BiFunction<T, Throwable, Boolean> handler,
									 long initialDelayMs,
									 long periodMs) {
		workerExecutor.scheduleAtFixedRate(() -> {
			try {
				T result = callable.call();
				executorToNotify.execute(() -> {
					if (handler.apply(result, null)) {
						executorToNotify.execute(onNotificationAction);
					}
				});
			} catch (Throwable t) {
				handler.apply(null, t);
			}
		}, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
	}
}
