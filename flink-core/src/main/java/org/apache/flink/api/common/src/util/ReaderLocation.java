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

package org.apache.flink.api.common.src.util;

import javax.annotation.Nullable;

/**
 * A description of the location of a source reader.
 * The location can be used to make locality aware assignment of splits to readers.
 */
public class ReaderLocation {

	@Nullable
	private final String hostName;

	/**
	 * Creates a ReaderLocation, optionally with a hostname.
	 */
	public ReaderLocation(@Nullable String hostName) {
		this.hostName = hostName;
	}

	@Nullable
	public String hostName() {
		return hostName;
	}

	@Override
	public String toString() {
		return "ReaderLocation: hostname=" + hostName;
	}
}
