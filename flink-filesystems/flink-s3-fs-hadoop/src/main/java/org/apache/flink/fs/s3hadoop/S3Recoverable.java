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

import org.apache.flink.core.fs.ResumableWriter.ResumeRecoverable;

import com.amazonaws.services.s3.model.PartETag;

import java.util.List;

/**
 * Data object to recover an S3 recoverable output stream.
 */
class S3Recoverable implements ResumeRecoverable {

	private final String uploadId;

	private final String bucket;

	private final String key;

	private final List<PartETag> parts;

	S3Recoverable(String uploadId, String bucket, String key, List<PartETag> parts) {
		this.uploadId = uploadId;
		this.bucket = bucket;
		this.key = key;
		this.parts = parts;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder(128);
		buf.append("S3Recoverable: ");
		buf.append(bucket).append('/').append(key);
		buf.append(" - ").append(uploadId).append(" [");

		int num = 0;
		for (PartETag part : parts) {
			if (0 != num++) {
				buf.append(", ");
			}
			buf.append(part.getPartNumber()).append('=').append(part.getETag());
		}
		buf.append(']');
		return buf.toString();
	}
}
