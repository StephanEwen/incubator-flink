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

import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@NotThreadSafe
public class S3RecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

	/** Logger for S3RecoverableFsDataOutputStream and its subclasses. */
	static final Logger LOG = LoggerFactory.getLogger(S3RecoverableFsDataOutputStream.class);

	private final ReentrantLock lock = new ReentrantLock();

	private final RecoverableS3MultiPartUpload upload;

	private final Supplier<Tuple2<File, OutputStream>> tempFileCreator;

	private final int partSize;

	private final byte[] buffer;

	/** Current position in the buffer, must be in [0, buffer.length]. */
	private int pos;

	private int bytesInCurrentPart;

	private long bytesBeforeCurrentPart;

	private OutputStream currentOut;

	private RefCountedFile currentTempFile;

	private volatile boolean closed;

	private S3RecoverableFsDataOutputStream(
			RecoverableS3MultiPartUpload upload,
			Supplier<Tuple2<File, OutputStream>> tempFileCreator,
			int partSize,
			int bufferSize) {

		checkArgument(partSize > 0);
		checkArgument(bufferSize > 0);

		this.upload = checkNotNull(upload);
		this.tempFileCreator = checkNotNull(tempFileCreator);
		this.partSize = partSize;
		this.buffer = new byte[bufferSize];
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
		}
	}

	@Override
	public void flush() throws IOException {
		// write the buffer to the stream
		currentOut.write(buffer, 0, pos);
		currentOut.flush();

		final int currPartSize = bytesInCurrentPart + pos;
		bytesInCurrentPart = currPartSize;
		pos = 0;

		// check if we start a new part
		if (currPartSize >= partSize) {
			// a new part should start from here
			lock();
			try {
				// guard against concurrent close() calls
				if (closed) {
					throw new IOException("stream is closed");
				}

				currentOut.close();

				// grab the current temp file and prevent it from being deleted
				final RefCountedFile partFile = currentTempFile;
				currentTempFile = null;

				bytesBeforeCurrentPart += currPartSize;
				bytesInCurrentPart = 0;

				upload.addPart(partFile, currPartSize);

				partFile.release();
				newPartTempFile();
			}
			finally {
				unlock();
			}
		}
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
			currentTempFile = null;
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
					upload.getWriteHelper(),
					snapshot.uploadId(),
					snapshot.key(),
					snapshot.parts(),
					snapshot.totalLength());
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

			if (currentTempFile != null) {
				currentTempFile.release();
				currentTempFile = null;
			}
		}
		finally {
			unlock();
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
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

	@GuardedBy("lock")
	private void newPartTempFile() throws IOException {
		checkState(currentTempFile == null, "current temp file is still written to");

		Tuple2<File, OutputStream> fileAndStream = tempFileCreator.get();
		currentTempFile = new RefCountedFile(fileAndStream.f0);
		currentOut = fileAndStream.f1;
		bytesInCurrentPart = 0;
	}

	// ------------------------------------------------------------------------
	//  factory methods
	// ------------------------------------------------------------------------

	public static S3RecoverableFsDataOutputStream newStream(
			RecoverableS3MultiPartUpload upload,
			Supplier<Tuple2<File, OutputStream>> tempFileCreator,
			int partSize,
			int bufferSize) {

	}

	// ------------------------------------------------------------------------
	//  committing / publishing result file
	// ------------------------------------------------------------------------

	static class S3Committer implements Committer {

		private final WriteOperationHelper writeHelper;

		private final String uploadId;

		private final String key;

		private final List<PartETag> parts;

		private final long totalLength;

		S3Committer(WriteOperationHelper writeHelper, String uploadId, String key, List<PartETag> parts, long totalLength) {
			this.writeHelper = writeHelper;
			this.uploadId = uploadId;
			this.key = key;
			this.parts = parts;
			this.totalLength = totalLength;
		}

		@Override
		public void commit() throws IOException {
			LOG.debug("Committing {} with MPU ID {}", key, uploadId);

			final AtomicInteger errorCount = new AtomicInteger();
			writeHelper.completeMPUwithRetries(key, uploadId, parts, totalLength, errorCount);

			if (errorCount.get() == 0) {
				LOG.debug("Successfully committed {} with MPU ID {}", key, uploadId);
			} else {
				LOG.debug("Successfully committed {} with MPU ID {} after {} retries.", key, uploadId, errorCount.get());
			}
		}

		@Override
		public void commitAfterRecovery() throws IOException {
			commit();
		}

		@Override
		public CommitRecoverable getRecoverable() {
			return new S3Recoverable(uploadId, key, parts, null, totalLength);
		}
	}
}
