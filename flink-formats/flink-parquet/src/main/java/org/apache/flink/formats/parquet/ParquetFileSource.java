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

import org.apache.flink.api.common.src.file.FileSource;
import org.apache.flink.api.common.src.file.FileSourceSplit;
import org.apache.flink.api.common.src.SplitReader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file source that reads Parquet files. Like most file sources, this source can
 * work as a batch source on a finite set of files, or as a continuous streaming source
 * that continuously monitors a directory for new files to appear.
 *
 * @param <T> The data type produced by the ParquetFileSource.
 */
public class ParquetFileSource<T> extends FileSource<T> {

	private static final long serialVersionUID = 1L;

	/** The builder for the parquet reader. This encapsulates the setup and configuration of the reader. */
	private final ParquetReaderFactory<T> readerFactory;

	/**
	 * Constructor for bounded (batch) input. Reads all files under the path.
	 *
	 * @param path          The file or directory to read.
	 * @param readerFactory The builder for the parquet reader. This encapsulates the setup
	 *                      and configuration of the reader.
	 */
	public ParquetFileSource(Path path, ParquetReaderFactory<T> readerFactory) {
		super(path);
		this.readerFactory = checkNotNull(readerFactory, "readerFactory");
	}

	/**
	 * Constructor for continuous streaming input. Continuously monitors for new files
	 * in the given interval.
	 *
	 * @param path               The file or directory to read.
	 * @param monitoringInterval The interval in which the source checks for new files.
	 * @param readerFactory      The builder for the parquet reader. This encapsulates the setup
	 *                           and configuration of the reader.
	 */
	public ParquetFileSource(Path path, Duration monitoringInterval, ParquetReaderFactory<T> readerFactory) {
		super(path, monitoringInterval);
		this.readerFactory = checkNotNull(readerFactory, "readerFactory");
	}

	// ------------------------------------------------------------------------

	@Override
	protected SplitReader<T> createReader(FileSystem fs, FileSourceSplit split) throws IOException {
		final InputFile in = new FileSplitInputFile(fs, split);
		final ParquetReader<T> reader = readerFactory.createReader(in);
		return new ParquetSplitReader<>(reader);
	}
}
