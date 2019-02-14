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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.src.SourceSplit;

import java.util.Arrays;

/**
 * A locatable input split is an input split referring to input data which is located on one or more hosts.
 */
@Public
public abstract class LocatableSourceSplit implements SourceSplit {

	private static final String[] EMPTY_ARR = new String[0];

	/** The names of the hosts storing the data this input split refers to. */
	private final String[] hostnames;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new locatable input split that refers to a multiple host as its data location.
	 *
	 * @param hostnames The names of the hosts storing the data this input split refers to.
	 */
	public LocatableSourceSplit(String... hostnames) {
		this.hostnames = hostnames == null ? EMPTY_ARR : hostnames;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the names of the hosts storing the data this input split refers to.
	 */
	public String[] getHostNames() {
		return this.hostnames;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return Arrays.hashCode(hostnames);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final LocatableSourceSplit that = (LocatableSourceSplit) o;
		return Arrays.equals(hostnames, that.hostnames);
	}

	@Override
	public String toString() {
		return "LocatableSourceSplit @ " + Arrays.toString(hostnames);
	}
}
