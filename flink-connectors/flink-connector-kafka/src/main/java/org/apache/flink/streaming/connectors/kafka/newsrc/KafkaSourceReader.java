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
import org.apache.flink.impl.connector.source.RecordsWithSplitIds;
import org.apache.flink.impl.connector.source.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.impl.connector.source.synchronization.FutureNotifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.function.Supplier;


/**
 * The source reader for Kafka.
 */
public class KafkaSourceReader<K, V, T> extends SingleThreadMultiplexSourceReaderBase<
		ConsumerRecord<K, V>, T, KafkaPartition, PartitionState<K, V>> {

	public KafkaSourceReader(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<K, V>>> elementsQueue,
			Supplier<SplitReader<ConsumerRecord<K, V>, KafkaPartition>> splitFetcherSupplier,
			RecordEmitter<ConsumerRecord<K, V>, T, PartitionState<K, V>> recordEmitter) {
		super(futureNotifier, elementsQueue, splitFetcherSupplier, recordEmitter);
	}


	@Override
	protected void onSplitFinished(Collection<String> finishedSplitIds) {
		// Do nothing.
	}

	@Override
	protected PartitionState<K, V> initializedState(KafkaPartition split) {
		return new PartitionState<>(split.topicPartition(), split.offset(), split.leaderEpoch().orElse(-1));
	}

	@Override
	protected KafkaPartition toSplitType(String splitId, PartitionState<K, V> splitState) {
		return splitState.toKafkaPartition();
	}
}
