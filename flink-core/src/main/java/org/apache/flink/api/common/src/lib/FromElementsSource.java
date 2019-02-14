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

import org.apache.flink.api.common.src.Boundedness;
import org.apache.flink.api.common.src.ReaderStatus;
import org.apache.flink.api.common.src.Source;
import org.apache.flink.api.common.src.SourceContext;
import org.apache.flink.api.common.src.SourceOutput;
import org.apache.flink.api.common.src.SourceReader;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link Source} that returns a sequence of elements.
 */
public final class FromElementsSource<T> implements Source<T, ByteElementsSplit> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> serializer;

	private final ByteElementsSplit elements;

	@SafeVarargs
	public FromElementsSource(TypeSerializer<T> serializer, T... elements) throws IOException {
		this(checkNotNull(serializer), ByteElementsSplit.fromElements(serializer, elements));
	}

	FromElementsSource(TypeSerializer<T> serializer, ByteElementsSplit elements) {
		this.serializer = serializer;
		this.elements = elements;
	}

	// ------------------------------------------------------------------------
	//  source interface methods
	// ------------------------------------------------------------------------

	@Override
	public Boundedness getBoundedness() {
		return Boundedness.BOUNDED;
	}

	@Override
	public SourceReader<T, ByteElementsSplit> createReader(SourceContext ctx) {
		return new ElementsReader<>(ctx, serializer);
	}

	@Override
	public SimpleVersionedSerializer<ByteElementsSplit> getSplitSerializer() {
		return new ByteElementsSplit.Serializer();
	}

	// ------------------------------------------------------------------------
	//  source reader
	// ------------------------------------------------------------------------

	static final class ElementsReader<T> extends SynchronousSequentialReader<T, ByteElementsSplit> {

		private final DataInputDeserializer in = new DataInputDeserializer();

		private final TypeSerializer<T> serializer;

		ElementsReader(SourceContext sourceContext, TypeSerializer<T> serializer) {
			super(sourceContext);
			this.serializer = serializer;
		}

		@Override
		protected ReaderStatus emitNext(ByteElementsSplit currentSplit, SourceOutput<T> output) throws IOException {
			final byte[] next = currentSplit.pollNextElement();
			if (next != null) {
				in.setBuffer(next);
				output.emitRecord(serializer.deserialize(in));
				return ReaderStatus.MORE_AVAILABLE;
			}
			else {
				return ReaderStatus.END_OF_SPLIT_DATA;
			}
		}
	}

	// ------------------------------------------------------------------------
	//  split enumerator and assigner
	// ------------------------------------------------------------------------
}
