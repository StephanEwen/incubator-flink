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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A utility class that creates temporary files (and output streams) within some
 * given temp directories.
 */
@ThreadSafe
public class TempFileCreator implements SupplierWithException<Tuple2<File, OutputStream>, IOException>  {

	private final File[] tempDirectories;

	private final AtomicInteger next;

	/**
	 * Creates a new TempFileCreator.
	 *
	 * @param tempDirectories The temp directories, not null, and at least one.
	 */
	public TempFileCreator(File... tempDirectories) {
		checkArgument(tempDirectories.length > 0, "tempDirectories must not be empty");
		for (File f : tempDirectories) {
			if (f == null) {
				throw new IllegalArgumentException("tempDirectories contains null entries");
			}
		}

		this.tempDirectories = tempDirectories.clone();
		this.next = new AtomicInteger(new Random().nextInt(this.tempDirectories.length));
	}

	/**
	 * Gets the next temp file and stream to temp file.
	 * This creates the temp file atomically, making sure no previous file is overwritten.
	 *
	 * <p>This method is safe against concurrent use.
	 *
	 * @return A pair of temp file and output stream to that temp file.
	 * @throws IOException Thrown, if the stream to the temp file could not be opened.
	 */
	@Override
	public Tuple2<File, OutputStream> get() throws IOException {
		final File directory = tempDirectories[nextIndex()];

		while (true) {
			try {
				File file = new File(directory, ".tmp_" + UUID.randomUUID());
				OutputStream out = Files.newOutputStream(file.toPath(), StandardOpenOption.CREATE_NEW);
				return new Tuple2<>(file, out);
			}
			catch (FileAlreadyExistsException e) {
				// fall through the loop and retry
			}
		}
	}

	private int nextIndex() {
		int currIndex, newIndex;
		do {
			currIndex = next.get();
			newIndex = currIndex + 1;
			if (newIndex == tempDirectories.length) {
				newIndex = 0;
			}
		}
		while (!next.compareAndSet(currIndex, newIndex));

		return currIndex;
	}
}
