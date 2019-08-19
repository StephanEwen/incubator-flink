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

import org.apache.flink.impl.connector.source.RecordsBySplits;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.splitreader.SplitsAddition;
import org.apache.flink.impl.connector.source.splitreader.SplitsChange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

public class KafkaPartitionReader<K, V> implements SplitReader<ConsumerRecord<K, V>, KafkaPartition> {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionReader.class);
	/**
	 * The maximum time to block in an API call. this is to ensure the thread could be
	 * waken up timely.
	 */
	private static final long MAX_BLOCK_TIME_MS = 1000L;

	private final KafkaConsumer<K, V> consumer;
	private final Map<TopicPartition, Long> endOffSets;

	public KafkaPartitionReader(Configuration config) {
		Properties props = new Properties();
		config.addAllToProperties(props);
		this.consumer = new KafkaConsumer<>(props);
		this.endOffSets = new HashMap<>();
	}

	@Override
	public RecordsBySplits<ConsumerRecord<K, V>> fetch() {
		// It is possible that the fetch got waken up and the iterator has not finished yet.
		// In that case, we resume from the unfinished iterator rather than start from
		// beginning.
		RecordsBySplits<ConsumerRecord<K, V>> recordsByPartition = new RecordsBySplits<>();
		try {
			ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(MAX_BLOCK_TIME_MS));
			LOG.debug("Fetched {} records from {} partitions", records.count(), records.partitions().size());
			for (TopicPartition tp : records.partitions()) {
				List<ConsumerRecord<K, V>> recordsForPartition = records.records(tp);
				long endOffset = endOffSets.get(tp);
				if (recordsForPartition.get(recordsForPartition.size() - 1).offset() < endOffset) {
					recordsByPartition.addAll(tp.toString(), records.records(tp));
				} else {
					for (ConsumerRecord<K, V> record : recordsForPartition) {
						if (record.offset() < endOffset - 1) {
							recordsByPartition.add(tp.toString(), record);
						} else if (record.offset() == endOffset - 1) {
							recordsByPartition.add(tp.toString(), record);
							recordsByPartition.addFinishedSplit(tp.toString());
							unassign(tp);
							break;
						} else {
							recordsByPartition.addFinishedSplit(tp.toString());
							unassign(tp);
							break;
						}
					}
				}
			}
		} catch (WakeupException we) {
			LOG.debug("Waken up when fetching the records.");
		}
		return recordsByPartition;
	}

	@Override
	public void handleSplitsChanges(Queue<SplitsChange<KafkaPartition>> splitsChanges) {
		Set<TopicPartition> currentAssignments = consumer.assignment();
		List<TopicPartition> toConsume = new ArrayList<>(currentAssignments);
		Set<KafkaPartition> toSeek = new HashSet<>();
		while (!splitsChanges.isEmpty()) {
			SplitsChange<KafkaPartition> splitsChange = splitsChanges.poll();
			if (splitsChange instanceof SplitsAddition) {
				for (KafkaPartition kp : splitsChange.splits()) {
					if (!currentAssignments.contains(kp.topicPartition())) {
						toConsume.add(kp.topicPartition());
						endOffSets.put(kp.topicPartition(), kp.endOffset());
					} else {
						throw new IllegalStateException("Partition " + kp.topicPartition() + " is already assigned.");
					}
					toSeek.add(kp);
				}
			} else {
				splitsChange.splits().forEach(sc -> {
					toConsume.remove(sc.topicPartition());
					endOffSets.remove(sc.topicPartition());
				});
			}

			LOG.debug("Assigning partitions {}", toConsume);
			if (!toSeek.isEmpty()) {
				LOG.debug("Seeking on partitions {}", toSeek);
			}
			consumer.assign(toConsume);
			toSeek.forEach(kp -> consumer.seek(
					kp.topicPartition(),
					new OffsetAndMetadata(kp.offset(), kp.leaderEpoch(), null)));
		}
	}

	@Override
	public void wakeUp() {
		consumer.wakeup();
	}

	@Override
	public void configure(Configuration config) {

	}

	private void unassign(TopicPartition tp) {
		Set<TopicPartition> newAssignments = new HashSet<>(consumer.assignment());
		newAssignments.remove(tp);
		consumer.assign(newAssignments);
	}
}
