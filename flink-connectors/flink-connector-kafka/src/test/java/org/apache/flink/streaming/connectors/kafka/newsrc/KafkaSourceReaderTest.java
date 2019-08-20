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

import org.apache.flink.api.connectors.source.Boundedness;
import org.apache.flink.api.connectors.source.SourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.reader.RecordEmitter;
import org.apache.flink.impl.connector.source.reader.RecordsWithSplitIds;
import org.apache.flink.impl.connector.source.reader.SourceReaderOptions;
import org.apache.flink.impl.connector.source.SourceReaderTest;
import org.apache.flink.impl.connector.source.reader.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.impl.connector.source.reader.synchronization.FutureNotifier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.Assert.assertNull;

public class KafkaSourceReaderTest extends SourceReaderTest<KafkaPartition> {
	private static final String TOPIC_NAME = "KafkaSourceReaderTest";
	private static KafkaTestService kafkaService = new KafkaTestService();

	@BeforeClass
	public static void setup() throws Exception {
		kafkaService.prepare();
		kafkaService.createTopic(TOPIC_NAME, NUM_SPLITS, 1);
		prepareRecordsInPartition();
	}

	@AfterClass
	public static void shutdown() throws Exception {
		kafkaService.shutDownServices();
	}


	@Override
	protected SourceReader<Integer, KafkaPartition> createReader(Boundedness boundedness) {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<Integer, Integer>>> elementQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		Supplier<SplitReader<ConsumerRecord<Integer, Integer>, KafkaPartition>> partitionReaderFactory =
				() -> new KafkaPartitionReader<>(getConfig(Boundedness.BOUNDED));
		RecordEmitter<ConsumerRecord<Integer, Integer>, Integer, PartitionState<Integer, Integer>> recordEmitter =
				(element, output, splitState) -> {
					output.collect(element.value());
					splitState.maybeUpdate(element);
				};
		return new KafkaSourceReader<>(futureNotifier, elementQueue, partitionReaderFactory, recordEmitter);
	}

	@Override
	protected List<KafkaPartition> getSplits(int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
		List<KafkaPartition> kafkaPartitions = new ArrayList<>();
		for (int i = 0; i < numSplits; i++) {
			kafkaPartitions.add(getSplit(i, numRecordsPerSplit, boundedness));
		}
		return kafkaPartitions;
	}

	@Override
	protected KafkaPartition getSplit(int splitId, int numRecords, Boundedness boundedness) {
		return new KafkaPartition(new TopicPartition(TOPIC_NAME, splitId), 0, numRecords, -1);
	}

	@Override
	protected long getIndex(KafkaPartition split) {
		return split.offset();
	}

	private Configuration getConfig(Boundedness boundedness) {
		Configuration config = new Configuration();
		config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
		config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
		config.setString(SourceReaderOptions.SOURCE_READER_BOUNDEDNESS, boundedness.name());
		config.setString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaService.brokerConnectorStrings());
		config.setString(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		config.setString(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		return config;
	}

	private static void prepareRecordsInPartition() {
		AtomicReference<Throwable> exception = new AtomicReference<>(null);
		Properties props = kafkaService.getStandardProperties();
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		Producer<Integer, Integer> producer = new KafkaProducer<>(props);
		for (int i = 0; i < NUM_SPLITS; i++) {
			for (int j = 0; j < NUM_RECORDS_PER_SPLIT; j++) {
				ProducerRecord<Integer, Integer> record = new ProducerRecord<>(TOPIC_NAME, i, null, i * 10 + j);
				producer.send(record, (m, e) -> exception.set(e));
			}
		}
		producer.close();
		assertNull(exception.get());
	}
}
