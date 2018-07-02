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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 */
@Internal
public class HadoopRecoverableWriter implements ResumableWriter {

	private final org.apache.hadoop.fs.FileSystem fs;

	public HadoopRecoverableWriter(org.apache.hadoop.fs.FileSystem fs) {
		this.fs = checkNotNull(fs);
	}

	@Override
	public RecoverableFsDataOutputStream open(Path filePath) throws IOException {
		final org.apache.hadoop.fs.Path targetFile = HadoopFileSystem.toHadoopPath(filePath);
		final org.apache.hadoop.fs.Path tempFile = generateStagingTempFilePath(fs, targetFile);
		return new HadoopRecoverableFsDataOutputStream(fs, targetFile, tempFile);
	}

	@Override
	public RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
		if (recoverable instanceof HadoopFsRecoverable) {
			return new HadoopRecoverableFsDataOutputStream(fs, (HadoopFsRecoverable) recoverable);
		}
		else {
			throw new IllegalArgumentException(
					"LocalFileSystem cannot recover recoverable for other file system: " + recoverable);
		}
	}

	@Override
	public Committer recoverForCommit(CommitRecoverable recoverable) throws IOException {
		if (recoverable instanceof HadoopFsRecoverable) {
			return new HadoopRecoverableFsDataOutputStream.HadoopFsCommitter(fs, (HadoopFsRecoverable) recoverable);
		}
		else {
			throw new IllegalArgumentException(
					"LocalFileSystem cannot recover recoverable for other file system: " + recoverable);
		}
	}

	@Override
	public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
//		@SuppressWarnings("unchecked")
//		SimpleVersionedSerializer<CommitRecoverable> typedSerializer = (SimpleVersionedSerializer<CommitRecoverable>)
//				(SimpleVersionedSerializer<?>) LocalRecoverableSerializer.INSTANCE;
//
//		return typedSerializer;
		throw new UnsupportedOperationException();
	}

	@Override
	public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
//		@SuppressWarnings("unchecked")
//		SimpleVersionedSerializer<ResumeRecoverable> typedSerializer = (SimpleVersionedSerializer<ResumeRecoverable>)
//				(SimpleVersionedSerializer<?>) LocalRecoverableSerializer.INSTANCE;
//
//		return typedSerializer;
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean supportsResume() {
		return true;
	}

	@VisibleForTesting
	static org.apache.hadoop.fs.Path generateStagingTempFilePath(
			org.apache.hadoop.fs.FileSystem fs,
			org.apache.hadoop.fs.Path targetFile) throws IOException {

		checkArgument(targetFile.isAbsolute(), "targetFile must be absolute");

		final org.apache.hadoop.fs.Path parent = targetFile.getParent();
		final String name = targetFile.getName();

		checkArgument(parent != null, "targetFile must not be the root directory");

		while (true) {
			org.apache.hadoop.fs.Path candidate = new org.apache.hadoop.fs.Path(
					parent, "." + name + ".inprogress." + UUID.randomUUID().toString());
			if (!fs.exists(candidate)) {
				return candidate;
			}
		}
	}
}
