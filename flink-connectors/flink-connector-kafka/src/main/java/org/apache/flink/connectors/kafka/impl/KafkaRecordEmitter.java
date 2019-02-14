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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.src.RecordEmitter;
import org.apache.flink.api.common.src.SourceOutput;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A RecordEmitter for Kafka that deserializes records, emits them with their timestamps,
 * and updates the partition offset.
 *
 * @param <E> The type of the deserialized records.
 */
public final class KafkaRecordEmitter<E> implements RecordEmitter<E, ConsumerRecord<byte[], byte[]>, TopicPartitionOffset> {

	private final KafkaDeserializationSchema<E> serializer;

	public KafkaRecordEmitter(KafkaDeserializationSchema<E> serializer) {
		this.serializer = checkNotNull(serializer);
	}

	@Override
	public void emitRecord(
			ConsumerRecord<byte[], byte[]> record,
			SourceOutput<E> output,
			TopicPartitionOffset split) throws Exception {

		final E value = serializer.deserialize(record);

		output.emitRecord(value, record.timestamp());

		split.advanceOffset(record.offset());
	}
}
