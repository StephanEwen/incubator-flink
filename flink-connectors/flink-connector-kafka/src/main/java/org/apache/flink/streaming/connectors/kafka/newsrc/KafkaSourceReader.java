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

import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.SourceReaderBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The source reader for Kafka.
 */
public class KafkaSourceReader<K, V> extends SourceReaderBase<ConsumerRecord<K, V>, ConsumerRecord<K, V>, KafkaPartition> {

	private final Map<TopicPartition, OffsetAndLeaderEpoch<K, V>> states = new HashMap<>();

	@Override
	protected SplitReader<ConsumerRecord<K, V>, KafkaPartition> createSplitReader() {
		return new KafkaPartitionReader<>(configuration);
	}

	@Override
	protected void initializedState(KafkaPartition split) {
		states.put(
			split.topicPartition(),
			new OffsetAndLeaderEpoch<>(split.offset(), split.leaderEpoch().orElse(-1)));
	}

	@Override
	protected void updateState(ConsumerRecord<K, V> element) {
		TopicPartition tp = new TopicPartition(element.topic(), element.partition());
		states.get(tp).maybeUpdate(element);
	}

	@Override
	protected ConsumerRecord<K, V> convertToEmit(ConsumerRecord<K, V> element) {
		return element;
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

	private static class OffsetAndLeaderEpoch<K, V> {
		private long offset;
		private int leaderEpoch;

		private OffsetAndLeaderEpoch(long offset, int leaderEpoch) {
			this.offset = offset;
			this.leaderEpoch = leaderEpoch;
		}

		private void maybeUpdate(ConsumerRecord<K, V> record) {
			offset = record.offset();
			leaderEpoch = record.leaderEpoch().orElse(-1);
		}
	}
}
