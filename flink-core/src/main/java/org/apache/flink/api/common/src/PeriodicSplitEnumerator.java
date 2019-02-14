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

package org.apache.flink.api.common.src;

import org.apache.flink.annotation.Public;

import java.io.IOException;

/**
 * The PeriodicSplitEnumerator is a SplitEnumerator that is called periodically to
 * try and discover more splits.
 */
@Public
public interface PeriodicSplitEnumerator<SplitT, CheckpointT> extends SplitEnumerator<SplitT, CheckpointT> {

	/**
	 * Called periodically to discover further splits.
	 *
	 * @return Returns true if further splits were discovered, false if not.
	 */
	boolean discoverMoreSplits() throws IOException;

	/**
	 * Continuous enumeration is only applicable to unbounded sources.
	 */
	default boolean isEndOfInput() {
		return false;
	}
}
