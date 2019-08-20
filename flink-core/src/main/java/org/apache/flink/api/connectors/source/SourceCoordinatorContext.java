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

package org.apache.flink.api.connectors.source;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.CompletableFuture;

public interface SourceCoordinatorContext {

	MetricGroup metricGroup();

	/**
	 * Send a source event to a source reader. The source reader is identified by its subtask id.
	 *
	 * @param subtaskId the subtask id of the source reader to send this event to.
	 * @param event the source event to send.
	 * @return a completable future which tells the result of the sending.
	 */
	CompletableFuture<Boolean> sendEventToSourceOperator(int subtaskId, OperatorEvent event);

	/**
	 * Access the state for the enumerator state.
	 */
	<T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties);
}
