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

import org.apache.flink.impl.connector.source.RecordsWithSplitId;
import org.apache.flink.impl.connector.source.fetcher.SplitFinishedCallback;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.splitreader.SplitsAddition;
import org.apache.flink.impl.connector.source.splitreader.SplitsChange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaPartitionReader<K, V> implements SplitReader<ConsumerRecord<K, V>, KafkaPartition> {
	/**
	 * The maximum time to block in an API call. this is to ensure the thread could be
	 * waken up timely.
	 */
	private static final long MAX_BLOCK_TIME_MS = 1000L;

	private final KafkaConsumer<K, V> consumer;
	private Iterator<RecordsAndPartition<K, V>> unfinishedIter;

	/** A boolean indicating whether the reader has been waken up. */
	private volatile boolean wakenUp;

	public KafkaPartitionReader(Configuration config) {
		Properties props = new Properties();
		config.addAllToProperties(props);
		this.consumer = new KafkaConsumer<>(props);
		this.unfinishedIter = null;
	}

	@Override
	public void fetch(
		BlockingQueue<RecordsWithSplitId<ConsumerRecord<K, V>>> queue,
		Queue<SplitsChange<KafkaPartition>> splitsChanges,
		SplitFinishedCallback splitFinishedCallback) throws InterruptedException {
		maybeUpdatePartitionAndOffsets(splitsChanges);

		// It is possible that the fetch got waken up and the iterator has not finished yet.
		// In that case, we resume from the unfinished iterator rather than start from
		// beginning.
		Iterator<RecordsAndPartition<K, V>> iter;
		if (unfinishedIter == null) {
			ConsumerRecords<K, V> recrods = consumer.poll(Duration.ofMillis(MAX_BLOCK_TIME_MS));
			iter = toRecordsAndPartition(recrods).iterator();
		} else {
			iter = unfinishedIter;
		}

		// so we can act to wakeup flag.
		// Put all the records into the queue. Ensure the thread blocks up to MAX_BLOCK_tIME_MS
		RecordsAndPartition<K, V> current = null;
		boolean lastPutSucceeded = true;
		while(iter.hasNext() && !wakenUp) {
			if (lastPutSucceeded) {
				current = iter.next();
			}
			lastPutSucceeded = queue.offer(current, MAX_BLOCK_TIME_MS, TimeUnit.MILLISECONDS);
		}

		// Throw away the iterator if all the records has been put into the queue.
		if (!iter.hasNext()) {
			unfinishedIter = null;
		} else {
			unfinishedIter = iter;
		}

		// Reset the wakenUp flag.
		wakenUp = false;
	}

	@Override
	public void wakeUp() {
		wakenUp = true;
	}

	private void maybeUpdatePartitionAndOffsets(Queue<SplitsChange<KafkaPartition>> splitsChanges) {
		Set<TopicPartition> currentAssignments = consumer.assignment();
		List<TopicPartition> toConsume = new ArrayList<>(currentAssignments);
		Set<KafkaPartition> toSeek = new HashSet<>();
		while (!splitsChanges.isEmpty()) {
			SplitsChange<KafkaPartition> splitsChange = splitsChanges.poll();
			if (splitsChange instanceof SplitsAddition) {
				for (KafkaPartition kp : splitsChange.splits()) {
					if (!currentAssignments.contains(kp.topicPartition())) {
						toConsume.add(kp.topicPartition());
					} else {
						throw new IllegalStateException("Partition " + kp.topicPartition() + " is already assigned.");
					}
					toSeek.add(kp);
				}
			} else {
				splitsChange.splits().forEach(sc -> toConsume.remove(sc.topicPartition()));
			}

			consumer.assign(toConsume);
			toSeek.forEach(kp -> consumer.seek(
					kp.topicPartition(),
					new OffsetAndMetadata(kp.offset(), kp.leaderEpoch(), null)));
		}
	}

	@Override
	public void configure(Configuration config) {

	}

	private List<RecordsAndPartition<K, V>> toRecordsAndPartition(ConsumerRecords<K, V> consumerRecords) {
		List<RecordsAndPartition<K, V>> recordsAndPartitions = new ArrayList<>();
		for (TopicPartition tp : consumerRecords.partitions()) {
			recordsAndPartitions.add(new RecordsAndPartition<>(consumerRecords.records(tp), tp));
		}
		return recordsAndPartitions;
	}

	private static class RecordsAndPartition<K, V> implements RecordsWithSplitId<ConsumerRecord<K, V>> {
		private final TopicPartition tp;
		private final Collection<ConsumerRecord<K, V>> records;

		private RecordsAndPartition(Collection<ConsumerRecord<K, V>> records, TopicPartition tp) {
			this.records = records;
			this.tp = tp;
		}

		@Override
		public String splitId() {
			return tp.toString();
		}

		@Override
		public Collection<ConsumerRecord<K, V>> records() {
			return records;
		}
	}
}
