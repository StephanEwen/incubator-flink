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

import org.apache.flink.formats.parquet.ParquetReaderFactory;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.io.InputFile;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Convenience factories to create Parquet Avro Readers for the
 * {@link org.apache.flink.formats.parquet.ParquetFileSource}.
 */
public class ParquetAvroReaderFactory<T> implements ParquetReaderFactory<T> {

	/**
	 * Creates a ParquetReaderBuilder that produces Avro specific records.
	 */
	public static <T extends SpecificRecordBase> ParquetAvroReaderFactory<T> forSpecificRecord() {
		return new ParquetAvroReaderFactory<>(SpecificData::get);
	}

	/**
	 * Creates a ParquetReaderBuilder that produces Avro generic records.
	 */
	public static ParquetAvroReaderFactory<GenericRecord> forGenericRecord() {
		return new ParquetAvroReaderFactory<>(GenericData::get);
	}

	/**
	 * Creates a ParquetReaderBuilder that produces types through reflect record reading.
	 */
	public static <T> ParquetAvroReaderFactory<T> forReflectRecord() {
		return new ParquetAvroReaderFactory<>(ReflectData::get);
	}

	// ------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	protected final SerializableSupplier<GenericData> dataModelSupplier;

	protected SerializableSupplier<CompressionCodecFactory> compressionCodecSupplier;

	protected ParquetAvroReaderFactory(SerializableSupplier<GenericData> dataModel) {
		this.dataModelSupplier = checkNotNull(dataModel);

		// size hint is not used for decompression, irrelevant here
		this.compressionCodecSupplier = () -> HadoopCodecs.newFactory(0);
	}

	// ------------------------------------------------------------------------

	public ParquetAvroReaderFactory<T> withCodecFactory(SerializableSupplier<CompressionCodecFactory> codecFactory) {
		this.compressionCodecSupplier = checkNotNull(codecFactory, "codecFactory");
		return this;
	}

	// ------------------------------------------------------------------------

	@Override
	public ParquetReader<T> createReader(InputFile in) throws IOException {
		final GenericData dataModel = dataModelSupplier.get();
		final CompressionCodecFactory compressionCodecFactory = compressionCodecSupplier.get();

		return AvroParquetReader.<T>builder(in)
				.withDataModel(dataModel)
				.withCodecFactory(compressionCodecFactory)
				.build();
	}
}
