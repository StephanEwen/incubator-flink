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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.ResumableFsDataOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
class LocalResumableFsDataOutputStream extends ResumableFsDataOutputStream<LocalResumable> {

	private final File targetFile;

	private final File tempFile;

	private final FileOutputStream fos;

	LocalResumableFsDataOutputStream(File targetFile, File tempFile) throws IOException {
		this.targetFile = checkNotNull(targetFile);
		this.tempFile = checkNotNull(tempFile);
		this.fos = new FileOutputStream(tempFile);
	}

	LocalResumableFsDataOutputStream(LocalResumable resumable) throws IOException {
		this.targetFile = checkNotNull(resumable.targetFile());
		this.tempFile = checkNotNull(resumable.tempFile());
		this.fos = new FileOutputStream(this.tempFile, true);
		this.fos.getChannel().truncate(resumable.offset());
	}

	@Override
	public void write(int b) throws IOException {
		fos.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		fos.write(b, off, len);
	}

	@Override
	public void close() throws IOException {
		fos.close();
	}

	@Override
	public void flush() throws IOException {
		fos.flush();
	}

	@Override
	public void sync() throws IOException {
		fos.getFD().sync();
	}

	@Override
	public long getPos() throws IOException {
		return fos.getChannel().position();
	}

	@Override
	public LocalResumable persist() throws IOException {
		final long pos = getPos();
		sync();
		return new LocalResumable(targetFile, tempFile, pos);
	}

	@Override
	public void closeAndPublish() throws IOException {
		close();
		Files.move(tempFile.toPath(), targetFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
	}
}
