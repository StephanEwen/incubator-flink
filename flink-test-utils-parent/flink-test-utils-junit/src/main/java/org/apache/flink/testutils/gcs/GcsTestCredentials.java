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

package org.apache.flink.testutils.gcs;

import org.junit.Assume;
import org.junit.AssumptionViolatedException;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

/**
 * Access to credentials to access GCS buckets during integration tests.
 */
public class GcsTestCredentials {

	@Nullable
	private static final String GCS_TEST_BUCKET = System.getenv("IT_CASE_GCS_BUCKET");

	@Nullable
	private static final String GCS_TEST_ACCESS_TOKEN = System.getenv("IT_CASE_GCS_TOKEN");

	// ------------------------------------------------------------------------

	/**
	 * Checks whether GCS test credentials are available in the environment variables
	 * of this JVM.
	 */
	public static boolean credentialsAvailable() {
		return GCS_TEST_BUCKET != null && GCS_TEST_ACCESS_TOKEN != null;
	}

	/**
	 * Checks whether credentials are available in the environment variables of this JVM.
	 * If not, throws an {@link AssumptionViolatedException} which causes JUnit tests to be
	 * skipped.
	 */
	public static void assumeCredentialsAvailable() {
		Assume.assumeTrue("No GCS credentials available in this test's environment", credentialsAvailable());
	}

	/**
	 * Gets the GCS Access Token.
	 *
	 * <p>This method throws an exception if the token is not available. Tests should
	 * use {@link #assumeCredentialsAvailable()} to skip tests when credentials are not
	 * available.
	 */
	public static String getAccessToken() {
		if (GCS_TEST_ACCESS_TOKEN != null) {
			return GCS_TEST_ACCESS_TOKEN;
		}
		else {
			throw new IllegalStateException("GCS test access token not available");
		}
	}

	/**
	 * Gets the URI for the path under which all tests should put their data.
	 *
	 * <p>This method throws an exception if the bucket was not configured. Tests should
	 * use {@link #assumeCredentialsAvailable()} to skip tests when credentials are not
	 * available.
	 */
	public static String createTestUri() {
		if (GCS_TEST_BUCKET != null) {
			return "gs://" + GCS_TEST_BUCKET + "/test-" + UUID.randomUUID() + '/';
		}
		else {
			throw new IllegalStateException("GCS test bucket not available");
		}
	}

	public static String writeKeyFileAndGetPath(File parentDirectory) throws IOException {
		final String key = getAccessToken();
		final File keyFile = new File(parentDirectory, "key-" + UUID.randomUUID() + ".json");

		try (FileWriter out = new FileWriter(keyFile)) {
			out.write(key);
		}

		return keyFile.getAbsolutePath();
	}
}
