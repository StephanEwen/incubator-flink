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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

public class SourceCoordinatorContextImpl implements SourceCoordinatorContext {
	private ExecutorNotifier notifier;

	public SourceCoordinatorContextImpl(ExecutorNotifier notifier) {
		this.notifier = notifier;
	}

	@Override
	public MetricGroup metricGroup() {
		return null;
	}

	@Override
	public CompletableFuture<Boolean> sendEventToSourceReader(int subtaskId, SourceEvent event) {
		return null;
	}

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		return null;
	}

	@Override
	public int numSubtasks() {
		return 0;
	}

	@Override
	public void notifyNewAssignment() {
		notifier.notifyReady();
	}

	@Override
	public <T> void notifyNewAssignmentAsync(Callable<T> callable, BiFunction<T, Throwable, Boolean> handler) {
		notifier.notifyReadyAsync(callable, handler);
	}

	@Override
	public <T> void notifyNewAssignmentAsync(Callable<T> callable,
											 BiFunction<T, Throwable, Boolean> handler,
											 long initialDelay,
											 long period) {
		notifier.notifyReadyAsync(callable, handler, initialDelay, period);
	}

	@Override
	public CompletableFuture<Boolean> sendEventToSourceOperator(int subtaskId, OperatorEvent event) {
		return null;
	}
}
