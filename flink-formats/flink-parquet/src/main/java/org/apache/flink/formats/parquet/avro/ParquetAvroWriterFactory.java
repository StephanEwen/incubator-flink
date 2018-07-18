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

package org.apache.flink.formats.parquet.avro;

import org.apache.flink.formats.parquet.ParquetBuilder;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to create a {@link ParquetWriter}.
 * @param <T> The type of input elements.
 */
public class ParquetAvroWriterFactory<T> implements ParquetBuilder<T> {

	private static final long serialVersionUID = 1L;

	private final String schemaString;

	public ParquetAvroWriterFactory(String schemaString) {
		this.schemaString = checkNotNull(schemaString);
	}

	@Override
	public ParquetWriter<T> createWriter(OutputFile out) throws IOException {
		final Schema schema = new Schema.Parser().parse(schemaString);

		return AvroParquetWriter.<T>builder(out)
				.withSchema(schema)
				.build();
	}
}
