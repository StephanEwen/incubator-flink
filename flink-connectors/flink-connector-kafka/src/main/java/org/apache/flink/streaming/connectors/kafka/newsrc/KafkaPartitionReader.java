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
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
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
	private long splitEpoch;
	private Iterator<ConsumerRecord<K, V>> unfinishedIter;

	/** A boolean indicating whether the reader has been waken up. */
	private volatile boolean wakenUp;

	public KafkaPartitionReader(Configuration config) {
		Properties props = new Properties();
		config.addAllToProperties(props);
		this.consumer = new KafkaConsumer<>(props);
		this.splitEpoch = -1;
		this.unfinishedIter = null;
	}

	@Override
	public void fetch(
		BlockingQueue<ConsumerRecord<K, V>> queue,
		SplitsWithEpoch<KafkaPartition> splitsWithEpoch) throws InterruptedException {
		maybeUpdatePartitionAndOffsets(splitsWithEpoch);

		// It is possible that the fetch got waken up and the iterator has not finished yet.
		// In that case, we resume from the unfinished iterator rather than start from
		// beginning.
		Iterator<ConsumerRecord<K, V>> iter;
		if (unfinishedIter == null) {
			iter = consumer.poll(Duration.ofMillis(MAX_BLOCK_TIME_MS)).iterator();
		} else {
			iter = unfinishedIter;
		}

		// so we can act to wakeup flag.
		// Put all the records into the queue. Ensure the thread blocks up to MAX_BLOCK_tIME_MS
		ConsumerRecord<K, V> current = null;
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
		}

		// Reset the wakenUp flag.
		wakenUp = false;
	}

	@Override
	public void wakeUp() {
		wakenUp = true;
	}

	private void maybeUpdatePartitionAndOffsets(SplitsWithEpoch<KafkaPartition> splitsWithEpoch) {
		if (splitEpoch < splitsWithEpoch.epoch()) {
			Set<TopicPartition> currentAssignments = consumer.assignment();
			List<TopicPartition> toConsume = new ArrayList<>();
			Set<KafkaPartition> toSeek = new HashSet<>();
			for (KafkaPartition kp : splitsWithEpoch.splits()) {
				if (!currentAssignments.contains(kp.topicPartition())) {
					toConsume.add(kp.topicPartition());
				}
				toSeek.add(kp);
			}

			consumer.assign(toConsume);
			toSeek.forEach(kp -> consumer.seek(
				kp.topicPartition(),
				new OffsetAndMetadata(kp.offset(), kp.leaderEpoch(), null)));
		}
	}
}
