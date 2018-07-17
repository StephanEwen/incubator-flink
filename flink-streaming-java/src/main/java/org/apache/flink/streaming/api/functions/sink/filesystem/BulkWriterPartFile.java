/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;

import java.io.IOException;

/**
 * A handler for the currently open part file in a specific {@link Bucket}.
 * This also implements the {@link PartFileInfo}.
 */
@Internal
class BulkWriterPartFile<IN, BucketID> extends PartFileHandler<IN, BucketID> {

	private final BulkWriter<IN> writer;

	private BulkWriterPartFile(
			final BulkWriter<IN> writer,
			final BucketID bucketId,
			final RecoverableFsDataOutputStream currentPartStream,
			final long creationTime) {

		super(bucketId, currentPartStream, creationTime);

		this.writer = writer;
	}

	@Override
	void write(IN element, long currentTime) throws IOException {
		writer.addElement(element);
		markWrite(currentTime);
	}

	RecoverableWriter.ResumeRecoverable persist() throws IOException {
		throw new UnsupportedOperationException();
	}

	RecoverableWriter.CommitRecoverable closeForCommit() throws IOException {
		writer.close();
		return super.closeForCommit();
	}

	static class Factory<IN, BucketID> implements PartFileFactory<IN, BucketID> {

		private final BulkWriter.Factory<IN> writerFactory;

		Factory(BulkWriter.Factory<IN> writerFactory) {
			this.writerFactory = writerFactory;
		}

		@Override
		public PartFileHandler<IN, BucketID> resumeFrom(
				BucketID bucketId,
				RecoverableWriter fileSystemWriter,
				ResumeRecoverable resumable,
				long creationTime) throws IOException {

			return null;
		}

		@Override
		public PartFileHandler<IN, BucketID> openNew(
				BucketID bucketId,
				RecoverableWriter fileSystemWriter,
				Path path,
				long creationTime) throws IOException {

			RecoverableFsDataOutputStream stream = fileSystemWriter.open(path);
			BulkWriter<IN> writer = writerFactory.create(stream);

			return new BulkWriterPartFile<>(writer, bucketId, stream, creationTime);
		}
	}
}
