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

package org.apache.flink.formats.parquet;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.src.SourceOutput;
import org.apache.flink.api.common.src.SourceStatus;
import org.apache.flink.api.common.src.SplitReader;

import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SplitReader} that reads Parquet files.
 */
@PublicEvolving
public class ParquetSplitReader<E> implements SplitReader<E> {

	/** The internal Parquet reader. */
	private final ParquetReader<E> parquetReader;

	/**
	 * Creates a new ParquetBulkWriter wrapping the given ParquetWriter.
	 *
	 * @param parquetReader The ParquetReader to read from.
	 */
	public ParquetSplitReader(ParquetReader<E> parquetReader) {
		this.parquetReader = checkNotNull(parquetReader, "parquetReader");
	}

	@Override
	public void advanceBlocking() throws IOException {
		// do nothing. we actually advance in the 'emitNext()', because we expect this
		// to never block for more than a few milliseconds and we save going through
		// a lookahead variable that way
	}

	@Override
	public SourceStatus emitNext(SourceOutput<E> output) throws IOException {
		final E next = parquetReader.read();
		if (next != null) {
			output.emitRecord(next);
			return SourceStatus.MORE_AVAILABLE;
		}
		else {
			return SourceStatus.END_OF_STREAM;
		}
	}

	@Override
	public void wakeup() {
		// nothing to do
	}

	@Override
	public void close() throws IOException {
		parquetReader.close();
	}
}
