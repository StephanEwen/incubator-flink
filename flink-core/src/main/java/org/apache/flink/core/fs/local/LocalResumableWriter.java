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

package org.apache.flink.core.fs.local;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.ResumableFsDataOutputStream;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class LocalResumableWriter implements ResumableWriter<LocalResumable> {

	private final LocalFileSystem fs;

	public LocalResumableWriter(LocalFileSystem fs) {
		this.fs = checkNotNull(fs);
	}

	@Override
	public ResumableFsDataOutputStream<LocalResumable> open(Path filePath) throws IOException {
		final File targetFile = fs.pathToFile(filePath);
		final File tempFile = generateStagingTempFilePath(targetFile);
		return new LocalResumableFsDataOutputStream(targetFile, tempFile);
	}

	@Override
	public ResumableFsDataOutputStream<LocalResumable> resume(LocalResumable resumable) throws IOException {
		return new LocalResumableFsDataOutputStream(resumable);
	}

	@Override
	public SimpleVersionedSerializer<LocalResumable> getResumableSerializer() {
		return null;
	}

	/**
	 *
	 * @param targetFile
	 * @return
	 */
	public static File generateStagingTempFilePath(File targetFile) {
		checkArgument(targetFile.isAbsolute(), "targetFile must be absolute");
		checkArgument(targetFile.isDirectory(), "targetFile must not be a directory");

		final File parent = targetFile.getParentFile();
		final String name = targetFile.getName();

		checkArgument(parent != null, "targetFile must not be the root directory");

		while (true) {
			File candidate = new File(parent, ".tmp_" + name + '_' + UUID.randomUUID().toString());
			if (!candidate.exists()) {
				return candidate;
			}
		}
	}
}
