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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A handler for the currently open part file in a specific {@link Bucket}.
 * This also implements the {@link PartFileInfo}.
 */
@Internal
abstract class PartFileHandler<IN, BucketID> implements PartFileInfo<BucketID> {

	private final BucketID bucketId;

	private final long creationTime;

	protected final RecoverableFsDataOutputStream currentPartStream;

	private long lastUpdateTime;

	protected PartFileHandler(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream currentPartStream,
			final long creationTime) {

		Preconditions.checkArgument(creationTime >= 0L);
		this.bucketId = Preconditions.checkNotNull(bucketId);
		this.currentPartStream = Preconditions.checkNotNull(currentPartStream);
		this.creationTime = creationTime;
		this.lastUpdateTime = creationTime;
	}

	abstract void write(IN element, long currentTime) throws IOException;

	RecoverableWriter.ResumeRecoverable persist() throws IOException {
		return currentPartStream.persist();
	}

	RecoverableWriter.CommitRecoverable closeForCommit() throws IOException {
		return currentPartStream.closeForCommit().getRecoverable();
	}

	void dispose() {
		// we can suppress exceptions here, because we do not rely on close() to
		// flush or persist any data
		IOUtils.closeQuietly(currentPartStream);
	}

	void markWrite(long now) {
		this.lastUpdateTime = now;
	}

	@Override
	public BucketID getBucketId() {
		return bucketId;
	}

	@Override
	public long getCreationTime() {
		return creationTime;
	}

	@Override
	public long getSize() throws IOException {
		return currentPartStream.getPos();
	}

	@Override
	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	// ------------------------------------------------------------------------

	interface PartFileFactory<IN, BucketID> {

		PartFileHandler<IN, BucketID> resumeFrom(
			final BucketID bucketId,
			final RecoverableWriter fileSystemWriter,
			final RecoverableWriter.ResumeRecoverable resumable,
			final long creationTime) throws IOException;

		PartFileHandler<IN, BucketID> openNew(
			final BucketID bucketId,
			final RecoverableWriter fileSystemWriter,
			final Path path,
			final long creationTime) throws IOException;
	}
}
