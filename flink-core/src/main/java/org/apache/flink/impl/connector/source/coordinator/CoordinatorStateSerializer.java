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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The serializer for {@link CoordinatorState}.
 *
 * @param <SplitT> the type of splits.
 * @param <CheckpointT> the type of enumerator checkpoint.
 */
public class CoordinatorStateSerializer<SplitT extends SourceSplit, CheckpointT>
		implements SimpleVersionedSerializer<CoordinatorState<SplitT, CheckpointT>> {
	private final SimpleVersionedSerializer<SplitT> splitSerializer;
	private final SimpleVersionedSerializer<CheckpointT> enuemratorStateSerailzer;

	public CoordinatorStateSerializer(SimpleVersionedSerializer<SplitT> splitSerializer,
									  SimpleVersionedSerializer<CheckpointT> enuemratorStateSerailzer) {
		this.splitSerializer = splitSerializer;
		this.enuemratorStateSerailzer = enuemratorStateSerailzer;
	}

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(CoordinatorState<SplitT, CheckpointT> coordinatorState) throws IOException {
		return coordinatorState.toBytes(splitSerializer, enuemratorStateSerailzer);
	}

	@Override
	public CoordinatorState<SplitT, CheckpointT> deserialize(int version, byte[] serialized) throws IOException {
		if (version != getVersion()) {
			throw new IllegalStateException(
					String.format("The coordinator state is in version %d, expected version %d",
								  version, getVersion()));
		}
		return CoordinatorState.fromBytes(serialized, splitSerializer, enuemratorStateSerailzer);
	}
}
