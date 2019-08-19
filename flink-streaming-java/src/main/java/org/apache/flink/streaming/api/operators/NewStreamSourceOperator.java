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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connectors.source.Source;
import org.apache.flink.api.connectors.source.SourceOutput;
import org.apache.flink.api.connectors.source.SourceReader;
import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A new stream source operator for FLIP-27.
 */
//public class NewStreamSourceOperator<T, SplitT extends SourceSplit, CoordCkt> extends AbstractStreamOperator<T>
//	implements SourceReader<T, SplitT> {
//
//	private final Source<T, SplitT, CoordCkt> source;
//	private SourceReader<T, SplitT> sourceReader;
//	private SimpleVersionedSerializer<SplitT> splitSerializer;
//	private ListState<byte[]> readerState;
//
//	public NewStreamSourceOperator(Source<T, SplitT, CoordCkt> source) {
//		this.source = source;
//	}
//
//	@Override
//	public void open() throws Exception {
//		this.sourceReader = source.createReader(getOperatorConfig().getConfiguration(), null);
//		this.splitSerializer = source.getSplitSerializer();
//		this.readerState = getRuntimeContext().getListState(
//			new ListStateDescriptor<byte[]>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE));
//	}
//
//	@Override
//	public void start() {
//		sourceReader.start();
//	}
//
//	@Override
//	public Status pollNext(SourceOutput<T> sourceOutput) {
//		return sourceReader.pollNext(sourceOutput);
//	}
//
//	@Override
//	public List<SplitT> snapshotState() {
//		List<SplitT> splitStates = sourceReader.snapshotState();
//		try {
//			List<byte[]> state = new ArrayList<>();
//			for (SplitT splitState : splitStates) {
//				state.add(splitSerializer.serialize(splitState));
//			}
//			readerState.update(state);
//		} catch (Exception e) {
//			throw new RuntimeException("Encountered exception when persisting state of the source readers.");
//		}
//		return splitStates;
//	}
//
//	@Override
//	public CompletableFuture<?> available() {
//		return sourceReader.available();
//	}
//
//	@Override
//	public void addSplits(List<SplitT> splits) {
//		sourceReader.addSplits(splits);
//	}
//
//	@Override
//	public void handleOperatorEvents(SourceEvent sourceEvent) {
//
//	}
//}
