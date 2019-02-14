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
import org.apache.flink.core.fs.FSDataInputStream;

import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * An adapter to turn Flink's {@link FSDataInputStream} into a {@link SeekableInputStream}.
 */
@Internal
final class SeekableInputStreamAdapter extends SeekableInputStream {

	/** The Flink stream that is read from. */
	private final FSDataInputStream in;

	/**
	 * Create a new SeekableInputStreamAdapter.
	 *
	 * @param in The Flink that is read from.
	 */
	SeekableInputStreamAdapter(FSDataInputStream in) {
		this.in = checkNotNull(in, "in");
	}

	@Override
	public long getPos() throws IOException {
		return in.getPos();
	}

	@Override
	public void seek(long newPos) throws IOException {
		in.seek(newPos);
	}

	@Override
	public int read() throws IOException {
		return in.read();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return in.read(b, off, len);
	}

	@Override
	public int read(byte[] b) throws IOException {
		return in.read(b);
	}

	@Override
	public void readFully(byte[] bytes) throws IOException {
		readFully(bytes, 0, bytes.length);
	}

	@Override
	public void readFully(byte[] bytes, int start, int len) throws IOException {
		if (len < 0) {
			throw new IndexOutOfBoundsException();
		}

		int numSoFar = 0;
		while (numSoFar < len) {
			int num = in.read(bytes, start + numSoFar, len - numSoFar);
			if (num < 0) {
				throw new EOFException();
			}
			numSoFar += num;
		}
	}

	@Override
	public int read(ByteBuffer buf) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void readFully(ByteBuffer buf) throws IOException {
		throw new UnsupportedOperationException();
	}
}
