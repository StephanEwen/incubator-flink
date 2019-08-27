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

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.SplitEnumeratorContext;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;

import java.util.concurrent.CompletableFuture;

public interface SourceCoordinatorContext<SplitT extends SourceSplit> extends SplitEnumeratorContext<SplitT> {

	/**
	 * Send a source event to a source operator. The source operator is identified by its subtask id.
	 * This method is different from {@link #sendEventToSourceReader(int, SourceEvent)} that the
	 * latter is used by the {@link org.apache.flink.api.connectors.source.SplitEnumerator} to
	 * send {@link SourceEvent} to the {@link org.apache.flink.api.connectors.source.SourceReader}.
	 *
	 * @param subtaskId the subtask id of the source operator to send this event to.
	 * @param event the source event to send.
	 * @return a completable future which tells the result of the sending.
	 */
	CompletableFuture<Boolean> sendEventToSourceOperator(int subtaskId, OperatorEvent event);
}
