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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferEdge.SharedBufferEdgeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An entry in {@link SharedBuffer} that allows to store relations between different entries.
 */
public class SharedBufferNode {

	private final List<SharedBufferEdge> edges;

	public SharedBufferNode() {
		edges = new ArrayList<>();
	}

	private SharedBufferNode(List<SharedBufferEdge> edges) {
		this.edges = edges;
	}

	public List<SharedBufferEdge> getEdges() {
		return edges;
	}

	public void addEdge(SharedBufferEdge edge) {
		edges.add(edge);
	}

	@Override
	public String toString() {
		return "SharedBufferNode{" +
			"edges=" + edges +
			'}';
	}

	/** Serializer for {@link SharedBufferNode}. */
	public static class SharedBufferNodeSerializer extends TypeSerializerSingleton<SharedBufferNode> {

		private static final long serialVersionUID = -6687780732295439832L;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public SharedBufferNode createInstance() {
			return new SharedBufferNode();
		}

		@Override
		public SharedBufferNode copy(SharedBufferNode from) {
			return new SharedBufferNode(new ArrayList<>(from.edges));
		}

		@Override
		public SharedBufferNode copy(SharedBufferNode from, SharedBufferNode reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(SharedBufferNode record, DataOutputView target) throws IOException {
			final List<SharedBufferEdge> edges = record.getEdges();
			target.writeInt(edges.size());

			final SharedBufferEdgeSerializer ser = SharedBufferEdgeSerializer.INSTANCE;
			for (SharedBufferEdge edge : edges) {
				ser.serialize(edge, target);
			}
		}

		@Override
		public SharedBufferNode deserialize(DataInputView source) throws IOException {
			final int size = source.readInt();
			final ArrayList<SharedBufferEdge> edges = new ArrayList<>(size);

			final SharedBufferEdgeSerializer ser = SharedBufferEdgeSerializer.INSTANCE;
			for (int i = size; i > 0; --i) {
				edges.add(ser.deserialize(source));
			}
			return new SharedBufferNode(edges);
		}

		@Override
		public SharedBufferNode deserialize(SharedBufferNode reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			final int size = source.readInt();
			target.writeInt(size);

			final SharedBufferEdgeSerializer ser = SharedBufferEdgeSerializer.INSTANCE;
			for (int i = size; i > 0; --i) {
				ser.copy(source, target);
			}
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj.getClass().equals(SharedBufferNodeSerializer.class);
		}

		@Override
		public TypeSerializerSnapshot<SharedBufferNode> snapshotConfiguration() {
			return new SharedBufferNodeSerializerSnapshot();
		}

		/**
		 * Config snapshot for the SharedBufferNodeSerializer.
		 */
		public static class SharedBufferNodeSerializerSnapshot extends SimpleTypeSerializerSnapshot<SharedBufferNode> {

			public SharedBufferNodeSerializerSnapshot() {
				super(SharedBufferNodeSerializer.class);
			}
		}
	}
}
