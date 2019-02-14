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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.src.util.LocatableSourceSplit;
import org.apache.flink.core.fs.Path;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file input split provides information on a particular part of a file, possibly
 * hosted on a distributed file system and replicated among several hosts.
 */
@Public
public class FileSourceSplit extends LocatableSourceSplit {

	/** The path of the file this file split refers to. */
	private final Path file;

	/** The position of the first byte in the file split to process. */
	private final long start;

	/** The number of bytes in the file split to process. */
	private final long length;

	// --------------------------------------------------------------------------------------------

	/**
	 * Constructs a new file split, with the position at the start offset.
	 */
	public FileSourceSplit(Path file, long start, long length, String... hosts) {
		super(hosts);

		checkArgument(start >= 0, "start offset must be >= 0");
		checkArgument(length >= 0, "length offset must be >= 0");
		checkArgument(start + length >= 0, "overflow in start + length");

		this.file = checkNotNull(file);
		this.start = start;
		this.length = length;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the path of the file containing this split's data.
	 */
	public Path getPath() {
		return file;
	}

	/**
	 * Returns the position of the first byte in the file to process.
	 */
	public long getStart() {
		return start;
	}

	/**
	 * Returns the number of bytes in the file to process.
	 */
	public long getLength() {
		return length;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + file.hashCode();
		result = 31 * result + Long.hashCode(start);
		result = 31 * result + Long.hashCode(length);
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final FileSourceSplit that = (FileSourceSplit) o;

		return this.file.equals(that.file) &&
				this.start == that.start &&
				this.length == that.length &&
				super.equals(that); // hostnames last, expensive check
	}

	@Override
	public String toString() {
		return file + " [" + start + ", " + (start + length) + ") @ " + Arrays.toString(getHostNames());
	}
}
