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

package org.apache.flink.fs.s3hadoop;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link ResumableWriter} against S3.
 *
 * <p>
 *
 * <p>This class partially reuses utility classes and implementations from the Hadoop
 * project, specifically around configuring S3 requests and handling retries.
 */
public class S3ResumableWriter implements ResumableWriter {

	/** The minimum size for parts in a multipart upload. */
	private static final int MIN_PART_SIZE = 5 << 20;

	private final S3AFileSystem s3aFs;

	/** The client to access to the S3 bucket. */
	private final AmazonS3 s3;

	private final String bucket;

	private final CannedAccessControlList acl;

	private final Path workingDir;

	/** The size for each part in the multipart upload. */
	private final int partSize;

	/**
	 * Creates a new recoverable writer.
	 *
	 * @param s3 The client to access to the S3 bucket.
	 * @param bucket
	 * @param acl
	 */
	public S3ResumableWriter(
			AmazonS3 s3,
			String bucket,
			CannedAccessControlList acl,
			Path workingDir,
			int partSize) {

		checkArgument(partSize >= MIN_PART_SIZE);

		this.s3 = checkNotNull(s3);
		this.bucket = checkNotNull(bucket);
		this.acl = acl;
		this.workingDir = checkNotNull(workingDir);
		this.partSize = partSize;
	}

	@Override
	public RecoverableFsDataOutputStream open(Path path) throws IOException {
		final String key = pathToKey(path);

		InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(
				bucket,
				key,
				s3aFs.newObjectMetadata(-1));
		req.setCannedACL(acl);

		s3.initiateMultipartUpload()
		return null;
	}

	@Override
	public RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException {
		return null;
	}

	@Override
	public Committer recoverForCommit(CommitRecoverable resumable) throws IOException {
		return null;
	}

	@Override
	public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
		throw new UnsupportedOperationException();
	}

	@Override
	public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean supportsResume() {
		return true;
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private String pathToKey(Path path) {
		final Path absolutePath = path.isAbsolute() ? path : new Path(workingDir, path);
		return absolutePath.toUri().getPath().substring(1);
	}
}
