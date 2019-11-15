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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

/**
 * Mock / sample implementation of the SourceReaderOperator to illustrate interaction with
 * the OperatorEvent sending / receiving.
 */
public class SourceReaderOperator<OutT> extends AbstractStreamOperator<OutT> implements OperatorEventHandler {

	private static final long serialVersionUID = 1L;

	private transient OperatorEventGateway eventGateway;

	@Override
	public void open() throws Exception {
		super.open();

		final OperatorID opId = getOperatorID();

		// too long chain traversal, related to the fact that containing task is the "uber entrypoint".
		eventGateway = getContainingTask().getOperatorEventDispatcher().registerEventHandler(opId, this);
	}

	@Override
	public void handleOperatorEvent(OperatorEvent evt) {
		if (evt.getClass() == SourceEvents.AssignSplitEvent.class) {
			assignSplit(((SourceEvents.AssignSplitEvent) evt).sourceSplit());
		}
		else {
			throw new IllegalArgumentException("Unrecognized event: " + evt);
		}
	}

	private void assignSplit(String split) {
		// split assignment logic
	}

	private void requestNewSplit() {
		eventGateway.sendEventToCoordinator(new SourceEvents.RequestSplitEvent());
	}
}
