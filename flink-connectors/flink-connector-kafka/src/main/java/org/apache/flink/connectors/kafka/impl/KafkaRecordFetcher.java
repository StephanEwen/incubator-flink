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

package org.apache.flink.connectors.kafka.impl;

import org.apache.flink.api.common.src.multisplit.RecordsBySplit;
import org.apache.flink.api.common.src.multisplit.SplitConnection;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A connection to Kafka that polls records for various splits (= Kafka partitions).
 */
public class KafkaRecordFetcher extends SplitConnection<ConsumerRecord<byte[], byte[]>, TopicPartitionOffset> {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordFetcher.class);

	/** Function to get the records for a split (TopicPartition) from a pulled batch or records. */
	private static final BiFunction<ConsumerRecords<byte[], byte[]>, TopicPartitionOffset, Iterable<ConsumerRecord<byte[], byte[]>>>
			PARTITION_RECORDS_GETTER = (records, partition) -> records.records(partition.getKafkaTopicPartition());

	/** The kafka consumer that fetches the raw records. */
	private final KafkaConsumer<byte[], byte[]> consumer;

	/**
	 * Creates a KafkaRecordFetcher, a SynchronousSplitConnection to a number of Kafka partitions.
	 */
	public KafkaRecordFetcher(KafkaConsumer<byte[], byte[]> consumer) {
		this.consumer = checkNotNull(consumer);
	}

	// ------------------------------------------------------------------------

	@Nullable
	@Override
	public RecordsBySplit<ConsumerRecord<byte[], byte[]>, TopicPartitionOffset> pollNextRecords(
			Duration timeout) throws IOException {

		final ConsumerRecords<byte[], byte[]> records;
		try {
			records = consumer.poll(timeout);
		}
		catch (WakeupException e) {
			return null;
		}

		// no data during poll interval
		if (records == null) {
			return null;
		}

		return RecordsBySplit.forMultipleSplits(records, PARTITION_RECORDS_GETTER);
	}

	@Override
	public void wakeup() {
		consumer.wakeup();
	}

	@Override
	public void close() throws IOException {
		consumer.close();
	}

	public Collection<TopicPartitionOffset> assignPartitions(Collection<TopicPartitionOffset> partitions) {
		LOG.info("Assigning new partitions {}", partitions);

		final KafkaConsumer<byte[], byte[]> consumer = this.consumer;

		final Collection<TopicPartition> kafkaPartitions = partitions.stream()
				.map(TopicPartitionOffset::getKafkaTopicPartition)
				.collect(Collectors.toList());

		consumer.assign(kafkaPartitions);

		return partitions.stream()
				.map((tpo) -> seekPartition(consumer, tpo))
				.collect(Collectors.toList());
	}

	private static TopicPartitionOffset seekPartition(KafkaConsumer consumer, TopicPartitionOffset partition) {
		final TopicPartition kafkaPartition = partition.getKafkaTopicPartition();
		final long offset = partition.getOffset();
		final long newOffset;
		final String offsetOrigin;

		if (offset == KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET) {
			consumer.seekToBeginning(Collections.singletonList(kafkaPartition));
			newOffset = consumer.position(kafkaPartition) - 1;
			offsetOrigin = "earliest";
		}
		else if (offset == KafkaTopicPartitionStateSentinel.LATEST_OFFSET) {
			consumer.seekToEnd(Collections.singletonList(kafkaPartition));
			newOffset = consumer.position(kafkaPartition) - 1;
			offsetOrigin = "latest";
		}
		else if (offset == KafkaTopicPartitionStateSentinel.GROUP_OFFSET) {
			// the KafkaConsumer by default will automatically seek the consumer position
			// to the committed group offset
			newOffset = consumer.position(kafkaPartition) - 1;
			offsetOrigin = "group";
		}
		else {
			consumer.seek(kafkaPartition, offset + 1);
			newOffset = offset;
			offsetOrigin = "checkpointed";
		}

		LOG.info("Seeking partition {} to {} offset : {}", kafkaPartition, offsetOrigin, newOffset);

		return new TopicPartitionOffset(kafkaPartition, newOffset);
	}
}
