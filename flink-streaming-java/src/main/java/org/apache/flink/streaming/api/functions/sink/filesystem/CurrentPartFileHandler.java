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
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A handler for the currently open part file in a specific {@link Bucket}.
 * This also implements the {@link RollingPolicy.PartFileInfoHandler}.
 */
@Internal
class CurrentPartFileHandler<IN> implements RollingPolicy.PartFileInfoHandler {

	private final String bucketId;

	private long creationTime;

	private long lastUpdateTime;

	private RecoverableFsDataOutputStream currentPartStream;

	CurrentPartFileHandler(final String bucketId) {
		this.bucketId = Preconditions.checkNotNull(bucketId);
		this.creationTime = Long.MAX_VALUE;
		this.lastUpdateTime = Long.MAX_VALUE;
	}

	void resumeFrom(RecoverableFsDataOutputStream recoveredStream, long currentTime) {
		Preconditions.checkState(currentPartStream == null);
		this.currentPartStream = recoveredStream;
		this.creationTime = currentTime;
		this.lastUpdateTime = currentTime;
	}

	void open(RecoverableWriter fsWriter, Path path, long currentTime) throws IOException {
		Preconditions.checkState(currentPartStream == null);
		this.currentPartStream = fsWriter.open(path);
		this.creationTime = currentTime;
	}

	void write(IN element, Encoder<IN> encoder, long currentTime) throws IOException {
		encoder.encode(element, currentPartStream);
		this.lastUpdateTime = currentTime;
	}

	RecoverableWriter.ResumeRecoverable persist() throws IOException {
		RecoverableWriter.ResumeRecoverable resumable = null;
		if (currentPartStream != null) {
			resumable = currentPartStream.persist();
		}
		return resumable;
	}

	RecoverableWriter.CommitRecoverable close() throws IOException {
		RecoverableWriter.CommitRecoverable commitRecoverable = null;
		if (currentPartStream != null) {
			commitRecoverable = currentPartStream.closeForCommit().getRecoverable();
			creationTime = Long.MAX_VALUE;
			lastUpdateTime = Long.MAX_VALUE;
			currentPartStream = null;
		}
		return commitRecoverable;
	}

	@Override
	public boolean isOpen() {
		return currentPartStream != null;
	}

	@Override
	public String getBucketId() {
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
}
