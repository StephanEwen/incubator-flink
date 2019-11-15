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

package org.apache.flink.streaming.runtime.operators.src;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.concurrent.CompletableFuture;

/**
 * Placeholder class for the SourceCoordinator.
 */
public class SourceCoordinator implements OperatorCoordinator {

	@Override
	public void start() throws Exception {}

	@Override
	public void finish() throws Exception {}

	@Override
	public void close() throws Exception {}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) {}

	@Override
	public void subtaskFailed(int subtask) {}

	@Override
	public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) throws Exception {
		return null;
	}

	@Override
	public void checkpointComplete(long checkpointId) throws Exception {}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {}
}
