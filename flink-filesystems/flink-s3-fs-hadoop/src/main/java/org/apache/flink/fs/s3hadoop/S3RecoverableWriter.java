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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.s3hadoop.utils.BackPressuringExecutor;
import org.apache.flink.fs.s3hadoop.utils.TempFileCreator;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link ResumableWriter} against S3.
 *
 * <p>This implementation makes heavy use of MultiPart Uploads in S3 to persist
 * intermediate data as soon as possible.
 *
 * <p>This class partially reuses utility classes and implementations from the Hadoop
 * project, specifically around configuring S3 requests and handling retries.
 */
public class S3RecoverableWriter implements ResumableWriter {

	private final S3AFileSystem s3aFs;

	private final S3AccessHelper s3access;

	private final Executor uploadThreadPool;

	private final TempFileCreator tempFileCreator;

	/** The number of bytes at which new MPU parts are started between checkpoints. */
	private final int rollingPartSize;

	private final int maxConcurrentUploadsPerStream;

	private final int streamBufferSize;

	public S3RecoverableWriter(
			S3AFileSystem s3aFs,
			Executor uploadThreadPool,
			TempFileCreator tempFileCreator,
			int rollingPartSize,
			int maxConcurrentUploadsPerStream) {

		checkArgument(rollingPartSize >= RecoverableS3MultiPartUpload.MIN_PART_SIZE);

		this.s3aFs = checkNotNull(s3aFs);
		this.s3access = new S3AccessHelper(s3aFs, s3aFs.getConf());
		this.uploadThreadPool = checkNotNull(uploadThreadPool);
		this.tempFileCreator = checkNotNull(tempFileCreator);
		this.rollingPartSize = rollingPartSize;
		this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;
		this.streamBufferSize = 4096;
	}

	@Override
	public RecoverableFsDataOutputStream open(Path path) throws IOException {
		final String key = pathToKey(path);
		final String keyPrefixForTemp = tempKeyPrefixForKey(key);

		final String multiPartUploadId;
		try {
			multiPartUploadId = s3access.initiateMultiPartUpload(key);
		}
		catch (Exception e) {
			throw new IOException("Could not open stream: failed to initiate S3 Multipart Upload.", e);
		}

		final RecoverableS3MultiPartUpload upload = RecoverableS3MultiPartUpload.newUpload(
				s3access, limitExecutor(uploadThreadPool), multiPartUploadId, key, keyPrefixForTemp);

		return S3RecoverableFsDataOutputStream.newStream(
				upload, tempFileCreator, rollingPartSize, streamBufferSize);
	}

	@Override
	public S3RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
		if (!(recoverable instanceof S3Recoverable)) {
			throw new IllegalArgumentException(
					"S3 File System cannot recover recoverable for other file system: " + recoverable);
		}

		final S3Recoverable s3recoverable = (S3Recoverable) recoverable;

		// if we have an object for trailing data, download that to let the stream resume with it
		final String inProgressPartKey = s3recoverable.lastPartObject();
		final File inProgressPart = inProgressPartKey == null ? null :
				downloadLastDataChunk(inProgressPartKey, s3recoverable.lastPartObjectLength());

		// recover the Multipart Upload
		final RecoverableS3MultiPartUpload upload = RecoverableS3MultiPartUpload.recoverUpload(
				s3access,
				limitExecutor(uploadThreadPool),
				s3recoverable.uploadId(),
				s3recoverable.key(),
				tempKeyPrefixForKey(s3recoverable.key()),
				s3recoverable.parts(),
				s3recoverable.numBytesInParts());

		// recover the output stream
		return S3RecoverableFsDataOutputStream.recoverStream(
				upload,
				tempFileCreator,
				rollingPartSize,
				streamBufferSize,
				inProgressPart,
				s3recoverable.numBytesInParts());
	}

	@Override
	public Committer recoverForCommit(CommitRecoverable recoverable) throws IOException {
		if (!(recoverable instanceof S3Recoverable)) {
			throw new IllegalArgumentException(
					"S3 File System cannot recover recoverable for other file system: " + recoverable);
		}

		final S3RecoverableFsDataOutputStream recovered = recover((S3Recoverable) recoverable);
		return recovered.closeForCommit();
	}

	@Override
	public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
		@SuppressWarnings("unchecked")
		SimpleVersionedSerializer<CommitRecoverable> typedSerializer = (SimpleVersionedSerializer<CommitRecoverable>)
				(SimpleVersionedSerializer<?>) S3RecoverableSerializer.INSTANCE;

		return typedSerializer;
	}

	@Override
	public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
		@SuppressWarnings("unchecked")
		SimpleVersionedSerializer<ResumeRecoverable> typedSerializer = (SimpleVersionedSerializer<ResumeRecoverable>)
				(SimpleVersionedSerializer<?>) S3RecoverableSerializer.INSTANCE;

		return typedSerializer;
	}

	@Override
	public boolean supportsResume() {
		return true;
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private File downloadLastDataChunk(String key, long expectedLength) throws IOException {
		// download the file (simple way)

		final byte[] buffer = new byte[32 * 1024];
		final Tuple2<File, OutputStream> fileAndStream = tempFileCreator.get();
		final File file = fileAndStream.f0;

		long numBytes = 0L;

		try (OutputStream outStream = fileAndStream.f1;
				org.apache.hadoop.fs.FSDataInputStream inStream =
						s3aFs.open(new org.apache.hadoop.fs.Path('/' + key))) {

			int numRead;
			while ((numRead = inStream.read(buffer)) > 0) {
				outStream.write(buffer, 0, numRead);
				numBytes += numRead;
			}
		}

		// some sanity checks
		if (numBytes != file.length()) {
			throw new IOException(String.format("Error recovering writer: " +
							"Downloading the last data chunk file gives incorrect length. " +
							"File=%d bytes, Stream=%d bytes",
									file.length(), numBytes));
		}
		if (numBytes != expectedLength) {
			throw new IOException(String.format("Error recovering writer: " +
							"Downloading the last data chunk file gives incorrect length." +
							"File length is %d bytes, RecoveryData indicates %d bytes",
									numBytes, expectedLength));
		}

		return file;
	}

	private String pathToKey(Path path) {
		return s3aFs.pathToKey(HadoopFileSystem.toHadoopPath(path));
	}

	private Executor limitExecutor(Executor executor) {
		return maxConcurrentUploadsPerStream <= 0 ?
				executor :
				new BackPressuringExecutor(executor, maxConcurrentUploadsPerStream);
	}

	private static String tempKeyPrefixForKey(String key) {
		int lastSlash = key.lastIndexOf('/');
		String parent = lastSlash == -1 ? "" : key.substring(0, lastSlash + 1);
		return parent + '_' + key + "_tmp_";
	}
}
