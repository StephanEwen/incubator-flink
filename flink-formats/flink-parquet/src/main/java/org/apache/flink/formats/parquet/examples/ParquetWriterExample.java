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

package org.apache.flink.formats.parquet.examples;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Javadoc.
 */
public class ParquetWriterExample {

	public static void main(String... args) throws Exception {

		Logger log = LoggerFactory.getLogger(ParquetWriterExample.class);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> stream = env.fromElements("a", "b", "c");

		ParquetBuilder<String> parquetBuilder = new ParquetBuilder<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public ParquetWriter<String> createWriter(OutputFile out) throws IOException {
				return AvroParquetWriter
						.<String>builder(out)
						.withSchema(SpecificData.get().getSchema(String.class))
						.build();
			}
		};

		stream.addSink(
				StreamingFileSink.forBulkFormat(
						new Path("file:///Users/kkloudas/Desktop/test"),
						new ParquetWriterFactory<>(parquetBuilder))
		.build());
		env.execute();
	}
}
