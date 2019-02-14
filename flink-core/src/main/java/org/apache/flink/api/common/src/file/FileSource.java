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

package org.apache.flink.api.common.src.file;

import org.apache.flink.api.common.src.Boundedness;
import org.apache.flink.api.common.src.Source;
import org.apache.flink.api.common.src.SourceContext;
import org.apache.flink.api.common.src.SourceReader;
import org.apache.flink.api.common.src.SplitReader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The basis of all sources that read files from {@link FileSystem FileSystems}.
 *
 * <p>The source can work both as a bounded (batch) and unbounded (streaming) source.
 *
 *
 * @param <T> The type of elements produced by the source.
 */
public abstract class FileSource<T> implements Source<T, FileSourceSplit> {

	private static final long serialVersionUID = 1L;

	private final Path path;

	@Nullable
	private final Duration monitorInterval;

	private boolean allowSplittingFiles = false;

	// ------------------------------------------------------------------------

	/**
	 * Constructor for bounded (batch) input. Reads all files under the path.
	 *
	 * @param path The file or directory to read.
	 */
	public FileSource(Path path) {
		this.path = checkNotNull(path, "path");
		this.monitorInterval = null;
	}

	/**
	 * Constructor for continuous streaming input. Continuously monitors for new files
	 * in the given interval.
	 *
	 * @param path               The file or directory to read.
	 * @param monitoringInterval The interval in which the source checks for new files.
	 */
	public FileSource(Path path, Duration monitoringInterval) {
		this.path = checkNotNull(path, "path");
		this.monitorInterval = checkNotNull(monitoringInterval, "monitoringInterval");

		checkArgument(!monitoringInterval.isNegative() && !monitoringInterval.isZero(),
				"the monitoring interval mist be positive and greater zero");
	}

	// ------------------------------------------------------------------------
	//  Parameters
	// ------------------------------------------------------------------------

	public FileSource<T> allowSplittingFiles(boolean allowSplit) {
		this.allowSplittingFiles = allowSplit;
		return this;
	}

	// ------------------------------------------------------------------------
	//  Source methods
	// ------------------------------------------------------------------------

	@Override
	public Boundedness getBoundedness() {
		return monitorInterval == null ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
	}

	@Override
	public SourceReader<T, FileSourceSplit> createReader(SourceContext ctx) {
		// the reader is the same, regardless of whether the source has a fix
		// number of splits to process, or continuously monitors for new files.
		return null;
	}

	protected abstract SplitReader<T> createReader(FileSystem fs, FileSourceSplit split) throws IOException;
}
