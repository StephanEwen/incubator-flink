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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.ResumableWriter.CommitRecoverable;

import java.io.IOException;
import java.util.List;

public class S3RecoverableFsDataOutputStream {



	private static class S3Committer implements Committer {

		private final String uploadId;

		private final String bucket;

		private final String key;

		private final List<PartETag> parts;

		@Override
		public void commit() throws IOException {

		}

		@Override
		public void commitAfterRecovery() throws IOException {

		}

		@Override
		public CommitRecoverable getRecoverable() {
			return null;
		}
	}
}
