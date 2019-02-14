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
 * The boundedness of the source: "bounded" for the currently available data (batch style),
 * "continuous unbounded" for a continuous streaming style source.
 */
@Public
public enum Boundedness {

	/**
	 * A bounded source processes the data that is currently available and will end after that.
	 *
	 * <p>When a source produces a bounded stream, the runtime may activate additional optimizations
	 * that are suitable only for bounded input. Incorrectly producing unbounded data when the source
	 * is set to produce a bounded stream will often result in programs that do not output any results
	 * and may eventually fail due to runtime errors (out of memory or storage).
	 */
	BOUNDED,

	/**
	 * A continuous unbounded source continuously processes all data as it comes.
	 *
	 * <p>The source may run forever (until the program is terminated) or might actually end at some point,
	 * based on some source-specific conditions. Because that is not transparent to the runtime,
	 * the runtime will use an execution mode for continuous unbounded streams whenever this mode
	 * is chosen.
	 */
	CONTINUOUS_UNBOUNDED
}
