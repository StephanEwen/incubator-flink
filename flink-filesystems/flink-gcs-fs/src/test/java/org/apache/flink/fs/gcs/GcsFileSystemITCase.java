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

package org.apache.flink.fs.gcs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.gcs.GcsTestCredentials;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the File System to GCS.
 *
 * <p>This test can only run when the required credentials are available.
 */
public class GcsFileSystemITCase extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMP = new TemporaryFolder();

	private static FileSystem fileSystem;

	private static Path basePath;

	@BeforeClass
	public static void checkCredentialsAndSetup() throws IOException {
		GcsTestCredentials.assumeCredentialsAvailable();

		basePath = new Path(GcsTestCredentials.createTestUri());

		Configuration configuration = new Configuration();
		configuration.setString(
				"google.cloud.auth.service.account.json.keyfile",
				GcsTestCredentials.writeKeyFileAndGetPath(TEMP.newFolder()));
		configuration.setBoolean("fs.gs.implicit.directory.repair", false);

		FileSystem.initialize(configuration);
		fileSystem = basePath.getFileSystem();
	}

	@AfterClass
	public static void cleanUp() throws Exception {
		try {
			if (fileSystem != null) {
				fileSystem.delete(basePath, true);
			}
		}
		finally {
			// reset configuration
			FileSystem.initialize(new Configuration());
		}
	}

	@Test
	public void testSimpleFileWriteAndRead() throws Exception {
		final Path path = new Path(basePath, "test.txt");

		final byte[] testBytes = "Hello GCS - Happy to meet you!".getBytes(StandardCharsets.UTF_8);

		try (FSDataOutputStream out = fileSystem.create(path, WriteMode.OVERWRITE)) {
			out.write(testBytes);
		}

		assertTrue(fileSystem.exists(path));

		final byte[] readBytes = new byte[testBytes.length];
		try (FSDataInputStream in = fileSystem.open(path)) {
			IOUtils.readFully(in, readBytes, 0, readBytes.length);
		}

		assertArrayEquals(testBytes, readBytes);

		fileSystem.delete(path, false);
		assertFalse(fileSystem.exists(path));
	}
}
