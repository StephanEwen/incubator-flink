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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;

/**
 * File System factory to create the FileSystem for gs:// .
 */
public class GcsFileSystemFactory implements FileSystemFactory {

	private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystemFactory.class);

	private static final String[] FORWARDED_KEYS = { "fs.gs.", "google.cloud." };

	@Nullable
	private Configuration flinkConfig;

	@Nullable
	private org.apache.hadoop.conf.Configuration hadoopConfig;

	@Override
	public String getScheme() {
		return "gs";
	}

	@Override
	public void configure(Configuration config) {
		flinkConfig = config;
		hadoopConfig = null;
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		LOG.debug("Creating GCS File System");

		try {
			// -- (1) create the init URI
			final URI initUri;
			if (fsUri.getScheme().equals("gs")) {
				initUri = fsUri;
			}
			else if (fsUri.getAuthority() != null) {
				initUri = new URI("gs://" + fsUri.getAuthority());
			}
			else {
				initUri = URI.create("gs:/");
			}

			// -- (2) get Hadoop config
			if (hadoopConfig == null) {
				hadoopConfig = getHadoopConfig(flinkConfig);
			}

			// -- (3) build the filesystem
			final GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
			fs.initialize(initUri, hadoopConfig);
			return new HadoopFileSystem(fs);
		}
		catch (IOException e) {
			throw e;
		}
		catch (Exception | LinkageError e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	private static org.apache.hadoop.conf.Configuration getHadoopConfig(@Nullable Configuration flinkConfig) {
		org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

		if (flinkConfig != null) {
			for (String key : flinkConfig.keySet()) {
				for (String keyPrefix : FORWARDED_KEYS) {
					if (key.startsWith(keyPrefix)) {
						hadoopConf.set(key, flinkConfig.getString(key, null));
					}
				}
			}
		}

		return hadoopConf;
	}
}
