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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A serializer for the {@link TopicPartitionOffset}.
 */
public final class TopicPartitionOffsetSerializer implements SimpleVersionedSerializer<TopicPartitionOffset> {

	public static final TopicPartitionOffsetSerializer INSTANCE = new TopicPartitionOffsetSerializer();

	private static final int VERSION = 1;

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public byte[] serialize(TopicPartitionOffset tpo) throws IOException {
		final byte[] topicBytes = tpo.getKafkaTopicPartition().topic().getBytes(StandardCharsets.UTF_8);
		final byte[] bytes = new byte[topicBytes.length + 16];

		final ByteBuffer buffer = ByteBuffer.wrap(bytes);

		buffer.putInt(topicBytes.length);
		buffer.put(topicBytes);
		buffer.putInt(tpo.getKafkaTopicPartition().partition());
		buffer.putLong(tpo.getOffset());

		return bytes;
	}

	@Override
	public TopicPartitionOffset deserialize(int version, byte[] serialized) throws IOException {
		if (version != VERSION) {
			throw new IOException("Unexpected version: " + version);
		}

		final ByteBuffer buffer = ByteBuffer.wrap(serialized);

		byte[] strBytes = new byte[buffer.getInt()];
		buffer.get(strBytes);

		final String topic = new String(strBytes, StandardCharsets.UTF_8);

		return new TopicPartitionOffset(
				new TopicPartition(topic, buffer.getInt()),
				buffer.getLong());
	}
}
