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
import org.apache.flink.api.connectors.source.SplitEnumerator;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.List;
import java.util.Map;

/**
 * The state to be checkpointed for the {@link SourceCoordinator}.
 *
 * @param <SplitT> the split type.
 * @param <CheckpointT> the {@link SplitEnumerator} state type.
 */
class CoordinatorState<SplitT extends SourceSplit, CheckpointT> {
	private final long checkpointId;
	private final SplitEnumerator<SplitT, CheckpointT> enumerator;
	private final SourceCoordinatorContext<SplitT> context;

	CoordinatorState(long checkpointId,
					 SplitEnumerator<SplitT, CheckpointT> enumerator,
					 SourceCoordinatorContext<SplitT> context) {
		this.checkpointId = checkpointId;
		this.enumerator = enumerator;
		this.context = context;
	}

	byte[] toBytes(SimpleVersionedSerializer<SplitT> splitSerializer,
				   SimpleVersionedSerializer<CheckpointT> enuemratorStateSerailzer) {
		return null;
	}

	static <SplitT extends SourceSplit, CheckpointT> CoordinatorState<SplitT, CheckpointT> fromBytes(
			byte[] bytes,
			SimpleVersionedSerializer<SplitT> splitSerializer,
			SimpleVersionedSerializer<CheckpointT> enuemratorStateSerailzer) {
		return null;
	}
}
