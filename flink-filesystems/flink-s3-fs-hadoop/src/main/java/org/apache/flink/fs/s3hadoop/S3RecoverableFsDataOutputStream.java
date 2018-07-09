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
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.ResumableWriter.CommitRecoverable;
import org.apache.flink.core.fs.ResumableWriter.ResumeRecoverable;
import org.apache.flink.fs.s3hadoop.utils.RefCountedFile;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.function.SupplierWithException;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A RecoverableFsDataOutputStream to S3 that is based on a recoverable multipart upload.
 *
 * <p>This class is NOT thread-safe. Concurrent writes tho this stream result in corrupt or
 * lost data.
 *
 * <p>The {@link #close()} method may be called concurrently when cancelling / shutting down.
 * It will still ensure that local transient resources (like streams and temp files) are cleaned up,
 * but will not touch data previously persisted in S3.
 */
@NotThreadSafe
class S3RecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

	/** Logger for S3RecoverableFsDataOutputStream and its subclasses. */
	static final Logger LOG = LoggerFactory.getLogger(S3RecoverableFsDataOutputStream.class);

	/** Lock that guards the critical sections when new parts are rolled over.
	 * Despite the class being declared not thread safe, we protect certain regions to
	 * at least enable concurrent close() calls during cancellation or abort/cleanup. */
	private final ReentrantLock lock = new ReentrantLock();

	private final RecoverableS3MultiPartUpload upload;

	private final SupplierWithException<Tuple2<File, OutputStream>, IOException> tempFileCreator;

	/** The number of bytes at which we start a new part between checkpoints. */
	private final int rollingPartSize;

	/** The write buffer. */
	private final byte[] buffer;

	/** Current position in the buffer, must be in [0, buffer.length]. */
	private int pos;

	private int bytesInCurrentPart;

	private long bytesBeforeCurrentPart;

	private OutputStream currentOut;

	private RefCountedFile currentTempFile;

	private volatile boolean closed;

	/**
	 * Single constructor to initialize all. Actual setup of the parts happens in the
	 * factory methods.
	 */
	private S3RecoverableFsDataOutputStream(
			RecoverableS3MultiPartUpload upload,
			SupplierWithException<Tuple2<File, OutputStream>, IOException> tempFileCreator,
			int rollingPartSize,
			int bufferSize,
			RefCountedFile inProgressPart,
			OutputStream currentPartOut,
			long bytesBeforeCurrentPart,
			int bytesInCurrentPart) {

		checkArgument(rollingPartSize > 0);
		checkArgument(bufferSize > 0);
		checkArgument(bytesBeforeCurrentPart >= 0);
		checkArgument(bytesInCurrentPart >= 0);
		checkArgument((inProgressPart == null) == (currentPartOut == null));

		this.upload = checkNotNull(upload);
		this.tempFileCreator = checkNotNull(tempFileCreator);
		this.rollingPartSize = rollingPartSize;
		this.buffer = new byte[bufferSize];
		this.currentTempFile = checkNotNull(inProgressPart);
		this.currentOut = checkNotNull(currentPartOut);
		this.bytesBeforeCurrentPart = bytesBeforeCurrentPart;
		this.bytesInCurrentPart = bytesInCurrentPart;
	}

	// ------------------------------------------------------------------------
	//  stream methods
	// ------------------------------------------------------------------------

	@Override
	public void write(int b) throws IOException {
		if (pos >= buffer.length) {
			flush();
		}

		buffer[pos++] = (byte) b;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (len < buffer.length) {
			if (len > buffer.length - pos) {
				flush();
			}
			System.arraycopy(b, off, buffer, pos, len);
			pos += len;
		}
		else {
			// special case for large writes: circumvent the internal buffer
			flush();
			currentOut.write(b, off, len);
			bytesInCurrentPart += len;

			checkStartNextPart(rollingPartSize);
		}
	}

	@Override
	public void flush() throws IOException {
		// write the buffer to the stream
		currentOut.write(buffer, 0, pos);
		currentOut.flush();

		bytesInCurrentPart += pos;
		pos = 0;

		checkStartNextPart(rollingPartSize);
	}

	@Override
	public long getPos() throws IOException {
		if (!closed) {
			return bytesBeforeCurrentPart + bytesInCurrentPart + pos;
		}
		else {
			throw new IOException("stream is closed");
		}
	}

	@Override
	public void sync() throws IOException {
		throw new UnsupportedOperationException("S3RecoverableFsDataOutputStream cannot sync state to S3. " +
				"Use persist() to create a persistent recoverable intermediate point.");
	}

	@Override
	public ResumeRecoverable persist() throws IOException {
		lock();
		try {
			// guard against concurrent close() calls
			if (closed) {
				throw new IOException("stream is closed");
			}

			// flush our memory buffer. this may result in a part being finalized.
			flush();

			// if the current part-in-progress has not yet reached the size to roll, but is
			// above the minimum threshold add it
			checkStartNextPart(RecoverableS3MultiPartUpload.MIN_PART_SIZE);

			// Add the trailing data if necessary. If the new part is empty (for example because
			// the previous flush just finished a part), we have no trailing data.
			// We do not stop writing to the current file, we merely limit the upload to the
			// first n bytes of the current file
			CompletableFuture<S3Recoverable> snapshotFuture = bytesInCurrentPart == 0 ?
					upload.snapshot() :
					upload.snapshotWithTrailingData(currentTempFile, bytesInCurrentPart);

			try {
				return snapshotFuture.get();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted waiting for upload snapshot to complete");
			}
			catch (ExecutionException e) {
				throw new IOException("Materialization of upload snapshot failed", e.getCause());
			}
		}
		finally {
			unlock();
		}
	}

	@Override
	public Committer closeForCommit() throws IOException {
		lock();
		try {
			if (closed) {
				throw new IOException("stream is closed");
			}

			// drain current buffer. this may finalize the current part.
			flush();

			// close the stream
			closed = true;
			pos = buffer.length; // make sure write methods fast after close
			currentOut.close();

			// if we have data in the last part, add it
			if (bytesInCurrentPart > 0) {
				upload.addLastPart(currentTempFile, bytesInCurrentPart);
			}

			currentTempFile.release();
			currentTempFile = null; // make sure close() does not release again
			bytesInCurrentPart = 0;

			// snapshot the upload and turn it into a committer
			final CompletableFuture<S3Recoverable> snapshotFuture = upload.snapshot();
			final S3Recoverable snapshot;
			try {
				snapshot = snapshotFuture.get();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted waiting for upload snapshot to complete");
			}
			catch (ExecutionException e) {
				throw new IOException("Materialization of upload snapshot failed", e.getCause());
			}

			return new S3Committer(
					upload.getS3AccessHelper(),
					snapshot.uploadId(),
					snapshot.key(),
					snapshot.parts(),
					snapshot.numBytesInParts());
		}
		finally {
			unlock();
		}
	}

	@Override
	public void close() throws IOException {
		lock();
		try {
			if (closed) {
				return;
			}

			closed = true;
			pos = buffer.length; // make sure write methods fast after close
			IOUtils.closeQuietly(currentOut);

			// release temp file if not already released
			if (currentTempFile != null) {
				currentTempFile.release();
				currentTempFile = null;
			}
		}
		finally {
			unlock();
		}
	}

	private void checkStartNextPart(int sizeThreshold) throws IOException {
		final int currPartSize = bytesInCurrentPart;

		// check if we start a new part
		if (currPartSize >= sizeThreshold) {
			// a new part should start from here
			lock();
			try {
				// guard against concurrent close() calls
				if (closed) {
					throw new IOException("stream is closed");
				}

				currentOut.close();
				bytesBeforeCurrentPart += currPartSize;
				bytesInCurrentPart = 0;

				// upload the current part file
				upload.addPart(currentTempFile, currPartSize);
				currentTempFile.release();

				// initialize a new temp file
				Tuple2<File, OutputStream> fileAndStream = tempFileCreator.get();
				currentTempFile = new RefCountedFile(fileAndStream.f0);
				currentOut = fileAndStream.f1;
			}
			finally {
				unlock();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  locking
	// ------------------------------------------------------------------------

	private void lock() throws IOException {
		try {
			lock.lockInterruptibly();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("interrupted");
		}
	}

	private void unlock() {
		lock.unlock();
	}

	// ------------------------------------------------------------------------
	//  factory methods
	// ------------------------------------------------------------------------

	public static S3RecoverableFsDataOutputStream newStream(
			RecoverableS3MultiPartUpload upload,
			SupplierWithException<Tuple2<File, OutputStream>, IOException> tempFileCreator,
			int rollingPartSize,
			int bufferSize) throws IOException {

		Tuple2<File, OutputStream> initialTempFile = tempFileCreator.get();

		return new S3RecoverableFsDataOutputStream(
				upload, tempFileCreator, rollingPartSize, bufferSize,
				new RefCountedFile(initialTempFile.f0), initialTempFile.f1,
				0, 0);
	}

	public static S3RecoverableFsDataOutputStream recoverStream(
			RecoverableS3MultiPartUpload upload,
			SupplierWithException<Tuple2<File, OutputStream>, IOException> tempFileCreator,
			int rollingPartSize,
			int bufferSize,
			@Nullable File inProgressPart,
			long bytesBeforeCurrentPart) throws IOException {

		final RefCountedFile currentPartFile;
		final OutputStream currentPartOut;
		final int currentPartLen;

		if (inProgressPart == null) {
			currentPartFile = null;
			currentPartOut = null;
			currentPartLen = 0;
		}
		else {
			currentPartFile = new RefCountedFile(inProgressPart);
			currentPartOut = Files.newOutputStream(inProgressPart.toPath(), StandardOpenOption.APPEND);
			currentPartLen = MathUtils.checkedDownCast(inProgressPart.length());
		}

		return new S3RecoverableFsDataOutputStream(
				upload, tempFileCreator, rollingPartSize, bufferSize,
				currentPartFile,
				currentPartOut,
				bytesBeforeCurrentPart,
				currentPartLen);
	}

	// ------------------------------------------------------------------------
	//  committing / publishing result file
	// ------------------------------------------------------------------------

	static class S3Committer implements Committer {

		private final S3AccessHelper s3access;

		private final String uploadId;

		private final String key;

		private final List<PartETag> parts;

		private final long totalLength;

		S3Committer(S3AccessHelper s3access, String uploadId, String key, List<PartETag> parts, long totalLength) {
			this.s3access = s3access;
			this.uploadId = uploadId;
			this.key = key;
			this.parts = parts;
			this.totalLength = totalLength;
		}

		@Override
		public void commit() throws IOException {
			LOG.info("Committing {} with MPU ID {}", key, uploadId);

			final AtomicInteger errorCount = new AtomicInteger();
			s3access.completeMPUwithRetries(key, uploadId, parts, totalLength, errorCount);

			if (errorCount.get() == 0) {
				LOG.debug("Successfully committed {} with MPU ID {}", key, uploadId);
			} else {
				LOG.debug("Successfully committed {} with MPU ID {} after {} retries.", key, uploadId, errorCount.get());
			}
		}

		@Override
		public void commitAfterRecovery() throws IOException {
			LOG.info("Trying to commit after recovery {} with MPU ID {}", key, uploadId);

			try {
				s3access.completeMPUwithRetries(key, uploadId, parts, totalLength, new AtomicInteger());
			}
			catch (IOException e) {
				LOG.info("Failed to commit after recovery {} with MPU ID {}. " +
								"Checking if file was committed before...", key, uploadId);
				LOG.trace("Exception when committing:", e);

				try {
					ObjectMetadata metadata = s3access.getObjectMetadata(key);
					if (totalLength != metadata.getContentLength()) {
						String message = String.format("Inconsistent result for object %s: conflicting lengths. " +
								"Recovered committer for upload %s indicates %s bytes, present object is %s bytes",
								key, uploadId, totalLength, metadata.getContentLength());
						LOG.warn(message);
						throw new IOException(message, e);
					}
				}
				catch (FileNotFoundException fnf) {
					LOG.warn("Object {} not existing after failed recovery commit with MPU ID {}", key, uploadId);
					throw new IOException(String.format("Recovering commit failed for object %s. " +
							"Object does not exist and MultiPart Upload %s is not valid.", key, uploadId), e);
				}
			}
		}

		@Override
		public CommitRecoverable getRecoverable() {
			return new S3Recoverable(uploadId, key, parts, totalLength);
		}
	}
}
