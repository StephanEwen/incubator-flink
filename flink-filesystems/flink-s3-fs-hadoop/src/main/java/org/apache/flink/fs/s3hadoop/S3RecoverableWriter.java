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

import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.s3hadoop.S3RecoverableFsDataOutputStream;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

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
public class S3RecoverableWriter implements ResumableWriter {

	/** The minimum size for parts in a multipart upload. */
	public static final int MIN_PART_SIZE = 5 << 20;

	private final S3AFileSystem s3aFs;

	private final WriteOperationHelper writeHelper;

	private final AmazonS3 s3client;

	private final Executor uploadThreadPool;

	private final Supplier<Tuple2<File, OutputStream>> tempFileCreator;

	/** The size for each part in the multipart upload. */
	private final int partSize;

	private final int streamBufferSize;

	public S3RecoverableWriter(
			S3AFileSystem s3aFs,
			Executor uploadThreadPool,
			int partSize,
			Supplier<Tuple2<File, OutputStream>> tempFileCreator) {

		checkArgument(partSize >= MIN_PART_SIZE);

		this.s3aFs = checkNotNull(s3aFs);
		this.partSize = partSize;

		this.writeHelper = new WriteOperationAccessor(s3aFs, s3aFs.getConf());
	}

	@Override
	public RecoverableFsDataOutputStream open(Path path) throws IOException {
		final String key = pathToKey(path);
		final String multiPartUploadId = writeHelper.initiateMultiPartUpload(key);

		final RecoverableS3MultiPartUpload upload = RecoverableS3MultiPartUpload.newUpload(
				writeHelper, uploadThreadPool, multiPartUploadId, key);

		return S3RecoverableFsDataOutputStream.newStream(
				upload, tempFileCreator, partSize, streamBufferSize);
	}

	@Override
	public RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
		if (!(recoverable instanceof S3Recoverable)) {
			throw new IllegalArgumentException(
					"LocalFileSystem cannot recover recoverable for other file system: " + recoverable);
		}

		final S3Recoverable s3recoverable = (S3Recoverable) recoverable;

		final RecoverableS3MultiPartUpload upload = RecoverableS3MultiPartUpload.recoverUpload(
				writeHelper,
				uploadThreadPool,

	}

	@Override
	public Committer recoverForCommit(CommitRecoverable recoverable) throws IOException {
		if (!(recoverable instanceof S3Recoverable)) {
			throw new IllegalArgumentException(
					"LocalFileSystem cannot recover recoverable for other file system: " + recoverable);
		}

		final S3Recoverable s3recoverable = (S3Recoverable) recoverable;

		final String uploadId = s3recoverable.uploadId();
		final String key = s3recoverable.key();

		final List<PartETag> origParts = s3recoverable.parts();
		final ArrayList<PartETag> parts;

		// convert trailing data object, if present
		final PartETag trailingPart = s3recover.lastPartObject() == null ?
				null : convertObjectToPart(s3recover.lastPartObject());

		// same list or parts, defensive copy
		if (trailingPart == null) {
			// same list of parts, but we make a defensive copy
			parts = new ArrayList<>(origParts);
		}
		else {
			parts = new ArrayList<>(origParts.size() + 1);
			parts.addAll(origParts);
			parts.add(trailingPart);
		}

		return new S3RecoverableFsDataOutputStream.S3Committer(
				writeHelper,
				uploadId,
				key,

		);
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

	private PartETag convertObjectToPart(String uploadId, String targetKey, String objectKey) throws IOException {
		final String bucket = s3aFs.getBucket();

		final CopyPartRequest copyRequest = new CopyPartRequest();
		copyRequest.setSourceBucketName(bucket);
		copyRequest.setSourceKey(objectKey);
		copyRequest.setDestinationBucketName(bucket);
		copyRequest.setDestinationKey(targetKey);

		// we should add retries here later
		final CopyPartResult copyResult = s3client.copyPart(copyRequest);

		return new PartETag(copyResult.getPartNumber(), copyResult.getETag());
	}

	private String pathToKey(Path path) {
		return s3aFs.pathToKey(HadoopFileSystem.toHadoopPath(path));
	}
}
