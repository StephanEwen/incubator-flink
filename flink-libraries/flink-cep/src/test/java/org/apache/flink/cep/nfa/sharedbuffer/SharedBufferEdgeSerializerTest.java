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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferEdge.SharedBufferEdgeSerializer;

import java.util.Objects;

/**
 * A test for the SharedBufferEdgeSerializer.
 */
public class SharedBufferEdgeSerializerTest extends SerializerTestBase<SharedBufferEdge> {

	@Override
	protected TypeSerializer<SharedBufferEdge> createSerializer() {
		return SharedBufferEdgeSerializer.INSTANCE;
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<SharedBufferEdge> getTypeClass() {
		return SharedBufferEdge.class;
	}

	@Override
	protected SharedBufferEdge[] getTestData() {
		return new SharedBufferEdge[] {
				new SharedBufferEdge(
						new NodeId(new EventId(0, 0L), "phil"),
						DeweyNumber.fromString("1")),
				new SharedBufferEdge(
						new NodeId(new EventId(1, 2L), "frank"),
						DeweyNumber.fromString("1.0")),
				new SharedBufferEdge(
						new NodeId(new EventId(-1, 458467846784L), "suzy"),
						DeweyNumber.fromString("1.4.6.3")),
		};
	}

	@Override
	protected boolean typesEquals(SharedBufferEdge a, SharedBufferEdge b) {
		return Objects.equals(a.getTarget(), b.getTarget()) &&
				Objects.equals(a.getDeweyNumber(), b.getDeweyNumber());
	}
}
