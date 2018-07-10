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

import org.apache.flink.fs.s3hadoop.utils.RefCountedFile;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An uploader for parts of a multipart upload. The uploader can snapshot its state to
 * be recovered after a failure.
 *
 * <p><b>Note:</b> This class is NOT thread safe and relies on external synchronization.
 *
 * <p><b>Note:</b> If any of the methods to add parts throws an exception, this class may be
 * in an inconsistent state (bookkeeping wise) and should be discarded and recovered.
 */
@NotThreadSafe
class RecoverableS3MultiPartUpload {

	/** The minimum size of a part in the multipart upload, except for the last part: 5 MIBytes. */
	public static final int MIN_PART_SIZE = 5 << 20;

	private final S3AccessHelper s3access;

	private final Executor uploadThreadPool;

	private final ArrayList<PartETag> completeParts;

	private final ArrayDeque<CompletableFuture<PartETag>> uploadsInProgress;

	private final String uploadId;

	private final String key;

	private final String keyPrefixForTempObjects;

	private int numParts;

	private long numBytes;

	// ------------------------------------------------------------------------

	private RecoverableS3MultiPartUpload (
			S3AccessHelper s3access,
			Executor uploadThreadPool,
			String uploadId,
			String key,
			String keyPrefixForTempObjects,
			ArrayList<PartETag> partsSoFar,
			long numBytes) {

		checkArgument(numBytes >= 0);

		this.s3access = checkNotNull(s3access);
		this.uploadThreadPool = checkNotNull(uploadThreadPool);
		this.uploadId = checkNotNull(uploadId);
		this.key = checkNotNull(key);
		this.keyPrefixForTempObjects = checkNotNull(keyPrefixForTempObjects);
		this.completeParts = checkNotNull(partsSoFar);
		this.numParts = partsSoFar.size();
		this.numBytes = numBytes;

		this.uploadsInProgress = new ArrayDeque<>();
	}

	// ------------------------------------------------------------------------

	/**
	 * Adds the next part to the uploads. This method checks that the part size is at
	 * least the minimum part size (except for the last part) and otherwise behaves like
	 * {@link #addLastPart(RefCountedFile, int)}.
	 *
	 * <p>This method is non-blocking and does not wait for the part upload to complete.
	 *
	 * @param file The file with the part data.
	 * @param length The number of bytes to take from the file.
	 *
	 * @throws IOException If this method throws an exception, the RecoverableS3MultiPartUpload
	 *                     should not be used any more, but recovered instead.
	 */
	void addPart(RefCountedFile file, int length) throws IOException {
		checkArgument(length >= MIN_PART_SIZE, "part too small");
		addLastPart(file, length);
	}

	/**
	 * Adds the last part to the uploads, which is allowed to be smaller than the
	 * {@link #MIN_PART_SIZE}. This will upload only the first bytes of the file, as
	 * passed through the length parameter.
	 *
	 * <p>This method is non-blocking and does not wait for the part upload to complete.
	 *
	 * @param file The file with the part data.
	 * @param length The number of bytes to take from the file.
	 *
	 * @throws IOException If this method throws an exception, the RecoverableS3MultiPartUpload
	 *                     should not be used any more, but recovered instead.
	 */
	void addLastPart(RefCountedFile file, int length) throws IOException {
		final UploadPartRequest uploadRequest = s3access.newUploadPartRequest(
				key, uploadId, numParts, length, null, file.getFile(), 0L);

		final CompletableFuture<PartETag> future = new CompletableFuture<>();
		uploadsInProgress.add(future);

		numParts++;
		numBytes += length;

		file.retain(); // keep the file while the async upload still runs
		uploadThreadPool.execute(new UploadTask(s3access, uploadRequest, file, future));
	}

	/**
	 * Creates a snapshot of this MultiPartUpload, from which the upload can be resumed.
	 *
	 * <p>This implementation currently blocks until all part uploads are complete and returns
	 * a completed future.
	 */
	CompletableFuture<S3Recoverable> snapshot() throws IOException {
		// this is currently blocking, to be made non-blocking in the future
		awaitPendingPartsUpload();

		S3Recoverable recoverable = new S3Recoverable(
				uploadId, key,
				new ArrayList<> (completeParts),
				numBytes);

		return CompletableFuture.completedFuture(recoverable);
	}

	/**
	 * Creates a snapshot of this MultiPartUpload, from which the upload can be resumed.
	 *
	 * <p>This implementation currently blocks until all part uploads are complete and returns
	 * a completed future.
	 */
	CompletableFuture<S3Recoverable> snapshotWithTrailingData(RefCountedFile file, long length) throws IOException {
		// first, upload the trailing data file. during that time, other in-progress uploads may complete.
		final String trailingDataObjectKey = createTempObjectKey();
		file.retain();
		try {
			final PutObjectRequest putRequest = s3access.createPutObjectRequest(
					trailingDataObjectKey,
					Files.newInputStream(file.getFile().toPath(), StandardOpenOption.READ),
					length);

			s3access.putObject(putRequest);
		}
		finally {
			file.release();
		}

		// make sure all other uploads are complete
		// this currently makes the method blocking, to be made non-blocking in the future
		awaitPendingPartsUpload();

		S3Recoverable recoverable = new S3Recoverable(
				uploadId, key,
				new ArrayList<> (completeParts),
				numBytes,
				trailingDataObjectKey,
				length);

		return CompletableFuture.completedFuture(recoverable);
	}

	S3AccessHelper getS3AccessHelper() {
		return s3access;
	}

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	private void awaitPendingPartsUpload() throws IOException {
		checkState(completeParts.size() + uploadsInProgress.size() == numParts);

		while (completeParts.size() < numParts) {
			CompletableFuture<PartETag> next = uploadsInProgress.peekFirst();
			PartETag nextPart;
			try {
				nextPart = next.get();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted while waiting for part uploads to complete");
			}
			catch (ExecutionException e) {
				throw new IOException("Uploading parts failed", e.getCause());
			}

			completeParts.add(nextPart);
			uploadsInProgress.removeFirst();
		}
	}

	private String createTempObjectKey() {
		return keyPrefixForTempObjects + UUID.randomUUID().toString();
	}

	// ------------------------------------------------------------------------
	//  factory methods
	// ------------------------------------------------------------------------

	public static RecoverableS3MultiPartUpload newUpload(
			S3AccessHelper s3access,
			Executor uploadThreadPool,
			String uploadId,
			String key,
			String keyPrefixForTempObjects) {

		return new RecoverableS3MultiPartUpload(
				s3access, uploadThreadPool,
				uploadId, key, keyPrefixForTempObjects,
				new ArrayList<>(), 0L);
	}

	public static RecoverableS3MultiPartUpload recoverUpload(
			S3AccessHelper s3access,
			Executor uploadThreadPool,
			String uploadId,
			String key,
			String keyPrefixForTempObjects,
			List<PartETag> partsSoFar,
			long numBytesSoFar) {

		return new RecoverableS3MultiPartUpload(
				s3access, uploadThreadPool,
				uploadId, key, keyPrefixForTempObjects,
				new ArrayList<>(partsSoFar), numBytesSoFar);

	}

	// ------------------------------------------------------------------------
	//  factory methods
	// ------------------------------------------------------------------------

	private static class UploadTask implements Runnable {

		private final S3AccessHelper s3access;

		private final UploadPartRequest uploadRequest;

		private final RefCountedFile file;

		private final CompletableFuture<PartETag> future;

		UploadTask(
				S3AccessHelper s3access,
				UploadPartRequest uploadRequest,
				RefCountedFile file,
				CompletableFuture<PartETag> future) {

			this.s3access = s3access;
			this.uploadRequest = uploadRequest;
			this.file = file;
			this.future = future;
		}

		@Override
		public void run() {
			try {
				UploadPartResult result = s3access.uploadPart(uploadRequest);
				future.complete(new PartETag(result.getPartNumber(), result.getETag()));
				file.release();
			}
			catch (Throwable t) {
				future.completeExceptionally(t);
			}
		}
	}

}
