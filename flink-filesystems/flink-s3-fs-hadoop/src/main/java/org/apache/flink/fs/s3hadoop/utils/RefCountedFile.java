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

package org.apache.flink.fs.s3hadoop.utils;

import org.apache.flink.fs.s3hadoop.S3RecoverableFsDataOutputStream;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file with a reference counter.
 * When the reference count goes to zero, the file is deleted.
 */
public class RefCountedFile {

	private final File file;

	private final AtomicInteger references;

	/**
	 * Creates a new file with a reference count starting at one.
	 */
	public RefCountedFile(File file) {
		this.file = checkNotNull(file);
		this.references = new AtomicInteger(1);
	}

	/**
	 * Gets the file. The file may be already deleted.
	 */
	public File getFile() {
		if (references.get() == 0) {
			throw new IllegalStateException("file has been deleted");
		}
		return file;
	}

	/**
	 * Adds a reference to the file.
	 *
	 * @throws IllegalStateException If the file is already deleted.
	 */
	public void retain() {
		int ref;
		do {
			ref = references.get();

			if (ref == 0) {
				throw new IllegalStateException("RefCountedFile is already released");
			}
		}
		while (!references.compareAndSet(ref, ref + 1));
	}

	/**
	 * Releases a reference to the file. If the number of references reaches zero,
	 * the file is deleted.
	 */
	public void release() {
		int ref;
		do {
			ref = references.get();

			if (ref == 0) {
				// already disposed
				return;
			}

		}
		while (!references.compareAndSet(ref, ref - 1));

		// delete the file if we removed the last reference
		if (ref == 1) {
			// counted down from one, so we release
			try {
				Files.deleteIfExists(file.toPath());
			}
			catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				S3RecoverableFsDataOutputStream.LOG.warn("Failed to delete temp file {}", file.getAbsolutePath());
			}
		}
	}
}
