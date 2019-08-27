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

package org.apache.flink.streaming.connectors.kafka.newsrc.enumerator;

import org.apache.flink.api.connectors.source.SplitEnumerator;
import org.apache.flink.api.connectors.source.SplitEnumeratorContext;
import org.apache.flink.api.connectors.source.SplitsAssignment;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.newsrc.KafkaPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaPartitionEnumerator implements SplitEnumerator<KafkaPartition, KafkaPartitionsCheckpoint> {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionEnumerator.class);
	private final Set<KafkaPartition> unassignedPartitions;
	private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
	private final Map<String, Integer> topicToNumPartitions;
	private SplitEnumeratorContext<KafkaPartition> context;

	public KafkaPartitionEnumerator(Configuration configuration) {
		Properties props = new Properties();
		configuration.addAllToProperties(props);
		this.kafkaConsumer = new KafkaConsumer<>(props);
		// This map needs to be thread safe because it may be accessed by two different threads.
		this.unassignedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());
		this.topicToNumPartitions = new HashMap<>();
	}

	@Override
	public void start() {
		context.notifyNewAssignmentAsync(
				this::checkNewPartitions,
				this::handleNewPartitions,
				0L,
				300_000L);
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
		// nothing to do yet.
	}

	@Override
	public void addSplitsBack(List<KafkaPartition> splits) {
		unassignedPartitions.addAll(splits);
		if (!unassignedPartitions.isEmpty()) {
			context.notifyNewAssignment();
		}
	}

	@Override
	public void updateAssignment() {
		if (!unassignedPartitions.isEmpty()) {
			Map<Integer, List<KafkaPartition>> additionalAssignment = new HashMap<>();
			for (KafkaPartition kp : unassignedPartitions) {
				int targetSubtask = kp.topicPartition().partition() % context.numSubtasks();
				if (context.registeredReaders().containsKey(targetSubtask)) {
					additionalAssignment.computeIfAbsent(targetSubtask, id -> new ArrayList<>())
										.add(kp);
				}
			}
			context.assignSplits(new SplitsAssignment<>(additionalAssignment));
		}
	}

	@Override
	public KafkaPartitionsCheckpoint snapshotState() {
		return null;
	}

	@Override
	public void setSplitEnumeratorContext(SplitEnumeratorContext<KafkaPartition> context) {
		this.context = context;
	}

	@Override
	public void close() throws IOException {
		kafkaConsumer.close();
	}

	private List<KafkaPartition> checkNewPartitions() {
		return null;
	}

	private boolean handleNewPartitions(List<KafkaPartition> newPartitions, Throwable t) {
		if (t == null) {
			return unassignedPartitions.addAll(newPartitions);
		} else {
			LOG.error("Caught unexpected exception when checking for new partition.", t);
			return false;
		}
	}
}
