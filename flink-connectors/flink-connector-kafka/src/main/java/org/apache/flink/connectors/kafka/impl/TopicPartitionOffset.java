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

import org.apache.flink.api.common.src.SourceSplit;

import org.apache.kafka.common.TopicPartition;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Kafka partition, with its current offset.
 */
public final class TopicPartitionOffset implements SourceSplit {

	private static final long serialVersionUID = 1L;

	private final TopicPartition kafkaTopicPartition;

	private long offset;

	public TopicPartitionOffset(TopicPartition kafkaTopicPartition, long offset) {
		this.kafkaTopicPartition = checkNotNull(kafkaTopicPartition);
		this.offset = offset;
	}

	// ------------------------------------------------------------------------

	public TopicPartition getKafkaTopicPartition() {
		return kafkaTopicPartition;
	}

	public long getOffset() {
		return offset;
	}

	public void advanceOffset(long newOffset) {
		if (newOffset > offset) {
			offset = newOffset;
		}
		else {
			throw new IllegalStateException(
					String.format("Offset not increasing: current=%d new=%d", offset, newOffset));
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return kafkaTopicPartition.toString() + " @ " + offset;
	}
}
