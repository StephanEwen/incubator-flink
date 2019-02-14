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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.src.file.FileSourceSplit;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of Parquet's {@link InputFile} interface that goes against
 * a Flink {@link FileSourceSplit}.
 */
@Internal
final class FileSplitInputFile implements InputFile {

	private final FileSystem fs;

	private final FileSourceSplit split;

	FileSplitInputFile(FileSystem fs, FileSourceSplit split) {
		this.fs = checkNotNull(fs);
		this.split = checkNotNull(split);
	}

	@Override
	public long getLength() throws IOException {
		return split.getLength();
	}

	@Override
	public SeekableInputStream newStream() throws IOException {
		final FSDataInputStream in = fs.open(split.getPath());
		in.seek(split.getStart());

		return new SeekableInputStreamAdapter(in);
	}
}
