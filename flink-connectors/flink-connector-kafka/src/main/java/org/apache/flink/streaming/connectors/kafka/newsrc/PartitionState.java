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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * The mutable partition state.
 */
public class PartitionState<K, V> {
	private TopicPartition tp;
	private long offset;
	private int leaderEpoch;

	PartitionState(TopicPartition tp, long offset, int leaderEpoch) {
		this.tp = tp;
		this.offset = offset;
		this.leaderEpoch = leaderEpoch;
	}

	void maybeUpdate(ConsumerRecord<K, V> record) {
		offset = record.offset();
		leaderEpoch = record.leaderEpoch().orElse(-1);
	}

	TopicPartition topicPartition() {
		return tp;
	}

	KafkaPartition toKafkaPartition() {
		return new KafkaPartition(tp, offset, leaderEpoch);
	}
}
