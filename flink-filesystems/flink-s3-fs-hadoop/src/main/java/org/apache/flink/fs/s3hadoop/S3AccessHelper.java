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

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;

import java.io.IOException;

/**
 * Utility class to access the {@link WriteOperationHelper}, which only has
 * a protected constructor.
 */
class S3AccessHelper extends WriteOperationHelper {

	private final S3AFileSystem s3a;

	S3AccessHelper(S3AFileSystem s3a, Configuration conf) {
		super(s3a, conf);
		this.s3a = s3a;
	}

	public ObjectMetadata getObjectMetadata(String key) throws IOException {
		try {
			return s3a.getObjectMetadata(new Path('/' + key));
		}
		catch (SdkBaseException e) {
			throw S3AUtils.translateException("getObjectMetadata", key, e);
		}
	}
}
