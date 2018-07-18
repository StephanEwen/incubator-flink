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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Tests for the {@link StreamingFileSink} with {@link BulkWriter}.
 */
public class BulkWriterTest extends TestLogger {

	
	private static class TestBulkWriter implements BulkWriter<String> {

		private static final Charset CHARSET = StandardCharsets.UTF_8;

		private final FSDataOutputStream stream;

		TestBulkWriter(final FSDataOutputStream stream) {
			this.stream = Preconditions.checkNotNull(stream);
		}

		@Override
		public void addElement(String element) throws IOException {
			stream.write(element.getBytes(CHARSET));
		}

		@Override
		public void flush() throws IOException {
			stream.sync();
			stream.flush();
		}

		@Override
		public void close() throws IOException {
			stream.close();
		}
	}

	private static class TestBulkWriterFactory implements BulkWriter.Factory<String> {

		private static final long serialVersionUID = 1L;

		@Override
		public BulkWriter<String> create(FSDataOutputStream out) throws IOException {
			return new TestBulkWriter(out);
		}
	}
}
