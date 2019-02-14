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

/**
 * The status returned by the {@link SourceReader} when emitting elements.
 */
@Public
public enum ReaderStatus {

	/**
	 * Indicator that more data is available and the reader can be called immediately again
	 * to emit more data.
	 */
	MORE_AVAILABLE,

	/**
	 * Indicator that no data is currently available, but more data will be available in the future
	 * in the readers splits.
	 *
	 * <p>This status can mean either that the reader is still in the process of asynchronously fetching
	 * the next data, or that the reader is waiting for more data to appear in the source (for example
	 * in the message queue).
	 */
	NOTHING_AVAILABLE,

	/**
	 * Indicator that the reader has reached the end of the data of its assigned splits.
	 * New splits should be added to the reader before calling it again to emit data.
	 *
	 * <p>This status is only returned by readers in cases where the splits are finite.
	 * Examples are files, static tables, or sealed message queue segments.
	 */
	END_OF_SPLIT_DATA
}
