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

package org.apache.flink.api.common.src.lib;

import org.apache.flink.api.common.src.SourceSplit;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A source split that is a sequence of byte arrays.
 * This split can be used to represent a sequence of serialized elements.
 */
public final class ByteElementsSplit implements SourceSplit, Serializable {

	private static final long serialVersionUID = 1L;

	private final ArrayDeque<byte[]> elements;

	ByteElementsSplit(ArrayDeque<byte[]> elements) {
		this.elements = elements;
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets (and removes) the next element from the split.
	 */
	public byte[] pollNextElement() {
		return elements.pollFirst();
	}

	/**
	 * Splits this split into a number of splits. The splits are disjoint and their
	 * union represents exactly the elements in this split.
	 */
	public List<ByteElementsSplit> splitInto(int numSplits) {
		final int elementsPerSplit = elements.size() / numSplits;
		final int splitsWithOneMore = elements.size() % numSplits;

		final ArrayList<ByteElementsSplit> splits = new ArrayList<>();
		final Iterator<byte[]> iter = elements.iterator();

		for (int i = splitsWithOneMore; i > 0; --i) {
			splits.add(new ByteElementsSplit(createDequeWithElements(iter, elementsPerSplit + 1)));
		}

		for (int i = numSplits - splitsWithOneMore; i > 0; --i) {
			splits.add(new ByteElementsSplit(createDequeWithElements(iter, elementsPerSplit)));
		}

		return splits;
	}

	private static <E> ArrayDeque<E> createDequeWithElements(Iterator<E> iter, int numElements) {
		ArrayDeque<E> deque = new ArrayDeque<>(numElements);
		for (int i = 0; i < numElements; i++) {
			deque.addLast(iter.next());
		}
		return deque;
	}

	// ------------------------------------------------------------------------
	//  factories
	// ------------------------------------------------------------------------

	@SafeVarargs
	public static <T> ByteElementsSplit fromElements(TypeSerializer<T> serializer, T... elements) throws IOException{
		final DataOutputSerializer out = new DataOutputSerializer(64);
		final ArrayDeque<byte[]> deque = new ArrayDeque<>(elements.length);

		for (T element : elements) {
			out.clear();
			serializer.serialize(element, out);
			deque.addLast(out.getCopyOfBuffer());
		}

		return new ByteElementsSplit(deque);
	}

	public static ByteElementsSplit fromBytes(List<byte[]> elementBytes) {
		final ArrayDeque<byte[]> elements = new ArrayDeque<>(elementBytes);
		return new ByteElementsSplit(elements);
	}

	// ------------------------------------------------------------------------
	//  serializer
	// ------------------------------------------------------------------------

	/**
	 * A serializer for this split type.
	 */
	public static final class Serializer implements SimpleVersionedSerializer<ByteElementsSplit> {

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(ByteElementsSplit split) throws IOException {
			final ArrayDeque<byte[]> elements = split.elements;

			final int numElements = elements.size();
			int numBytes = 4 + 4 * numElements;

			for (byte[] element : elements) {
				numBytes += element.length;
			}

			DataOutputSerializer ser = new DataOutputSerializer(numBytes);
			ser.writeInt(numElements);
			for (byte[] element : elements) {
				ser.writeInt(element.length);
				ser.write(element);
			}

			return ser.getSharedBuffer();
		}

		@Override
		public ByteElementsSplit deserialize(int version, byte[] serialized) throws IOException {
			if (version != 1) {
				throw new IOException("wrong version");
			}

			final DataInputDeserializer deser = new DataInputDeserializer(serialized);
			final int numElements = deser.readInt();

			final ArrayDeque<byte[]> elements = new ArrayDeque<>(numElements);
			for (int i = 0; i < numElements; i++) {
				final byte[] bytes = new byte[deser.readInt()];
				deser.readFully(bytes);
				elements.addLast(bytes);
			}

			return new ByteElementsSplit(elements);
		}
	}
}
