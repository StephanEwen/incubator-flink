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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * An implementation of Flink's file system interface wrapping Hadoop's s3a file system.
 */
public class S3FileSystem extends HadoopFileSystem {

	/** The client to access to the S3 bucket. */
	private final AmazonS3 s3;

	/**
	 * Wraps the given Hadoop File System object as a Flink File System object.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * @param s3a The Hadoop S3A FileSystem that will be used under the hood.
	 */
	public S3FileSystem(S3AFileSystem s3a) {
		super(s3a);
		this.s3 = getS3FromFileSystem(s3a);
	}

	// ------------------------------------------------------------------------
	//  File System methods
	// ------------------------------------------------------------------------

	@Override
	public ResumableWriter createRecoverableWriter() throws IOException {

	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static AmazonS3 getS3FromFileSystem(S3AFileSystem s3a) {
		try {
			Field field = S3AFileSystem.class.getDeclaredField("s3");
			field.setAccessible(true);
			return (AmazonS3) field.get(s3a);
		}
		catch (Exception e) {
			// something is fundamentally wrong with how this module is packaged
			throw new Error("Cannot access S3 client in s3a file system", e);
		}
	}
}
