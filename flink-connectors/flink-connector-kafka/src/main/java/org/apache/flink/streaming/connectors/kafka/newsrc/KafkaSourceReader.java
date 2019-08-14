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

package org.apache.flink.streaming.connectors.kafka.newsrc;

import org.apache.flink.impl.connector.source.RecordEmitter;
import org.apache.flink.impl.connector.source.fetcher.SplitFetcherManager;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.SourceReaderBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.newsrc.KafkaSourceReader.*;

/**
 * The source reader for Kafka.
 */
public class KafkaSourceReader<K, V> extends SourceReaderBase<ConsumerRecord<K, V>, ConsumerRecord<K, V>,
		KafkaPartition, PartitionState<K, V>> {

	private final Map<TopicPartition, PartitionState<K, V>> states = new HashMap<>();

	public KafkaSourceReader(SplitFetcherManager splitFetcherManager,
							 RecordEmitter recordEmitter) {
		super(splitFetcherManager, recordEmitter);
	}

	@Override
	protected SplitReader<ConsumerRecord<K, V>, KafkaPartition> createSplitReader() {
		return new KafkaPartitionReader<>(configuration);
	}

	@Override
	protected void initializedState(KafkaPartition split) {
		states.put(
			split.topicPartition(),
			new PartitionState<>(split.topicPartition(), split.offset(), split.leaderEpoch().orElse(-1)));
	}

	@Override
	protected KafkaPartition toSplitType(String splitId, PartitionState splitState) {
		return splitState.toKafkaPartition();
	}

	@Override
	public List<KafkaPartition> snapshotState() {
		List<KafkaPartition> snapshot = new ArrayList<>();
		states.entrySet().forEach(entry -> {
			OffsetAndLeaderEpoch value = entry.getValue();
			snapshot.add(new KafkaPartition(
				entry.getKey(),
				value.offset,
				value.leaderEpoch));
		});
		return snapshot;
	}

	@Override
	protected void onSplitFinished(String finishedSplitIds) {

	}
}
