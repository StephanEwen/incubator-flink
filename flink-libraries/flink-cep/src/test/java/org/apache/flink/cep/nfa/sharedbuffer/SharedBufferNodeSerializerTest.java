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
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferNode.SharedBufferNodeSerializer;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A test for the SharedBufferNodeSerializer.
 */
public class SharedBufferNodeSerializerTest extends SerializerTestBase<SharedBufferNode> {

	@Override
	protected TypeSerializer<SharedBufferNode> createSerializer() {
		return new SharedBufferNodeSerializer();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<SharedBufferNode> getTypeClass() {
		return SharedBufferNode.class;
	}

	@Override
	protected SharedBufferNode[] getTestData() {
		final SharedBufferNode node1 = new SharedBufferNode();
		node1.addEdge(
				new SharedBufferEdge(
						new NodeId(new EventId(0, 0L), "phil"),
						DeweyNumber.fromString("1")));
		node1.addEdge(
				new SharedBufferEdge(
						new NodeId(new EventId(1, 2L), "frank"),
						DeweyNumber.fromString("1.0")));
		node1.addEdge(
				new SharedBufferEdge(
						new NodeId(new EventId(-1, 458467846784L), "suzy"),
						DeweyNumber.fromString("1.4.6.3")));

		final SharedBufferNode node2 = new SharedBufferNode();
		node2.addEdge(
				new SharedBufferEdge(
						new NodeId(new EventId(-1, -1L), "caroline"),
						DeweyNumber.fromString("1.0.0.0.0.0.0.0.77")));
		node2.addEdge(
				new SharedBufferEdge(
						new NodeId(new EventId(99999, 57676555555L), "phobe"),
						DeweyNumber.fromString("1")));
		node2.addEdge(
				new SharedBufferEdge(
						new NodeId(new EventId(-1, 458467846784L), "andy"),
						DeweyNumber.fromString("0")));

		return new SharedBufferNode[] { node1, node2 };
	}

	@Override
	protected boolean typesEquals(SharedBufferNode a, SharedBufferNode b) {
		List<SharedBufferEdge> listA = a.getEdges();
		List<SharedBufferEdge> listB = a.getEdges();

		if (listA.size() != listB.size()) {
			return false;
		}

		Iterator<SharedBufferEdge> listBIter = listB.iterator();
		for (SharedBufferEdge aEdge : listA) {
			SharedBufferEdge bEdge = listBIter.next();
			if (!(Objects.equals(aEdge.getTarget(), bEdge.getTarget()) &&
					Objects.equals(aEdge.getDeweyNumber(), bEdge.getDeweyNumber()))) {
				return false;
			}
		}

		return true;
	}
}
