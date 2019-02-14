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

package org.apache.flink.api.common.eventtime;

import java.time.Duration;

/**
 * A watermark generator that assumes monotonically ascending timestamps within the
 * stream split and periodically generates watermarks based on that assumption.
 * In addition, if no event is observed within a certain time, the generator marks its
 * output as {@link WatermarkOutput#markIdle() idle}.
 *
 * <p>Idleness is determined as part of the periodic watermark generation. Hence the time it
 * takes for the output to be marked as idle may be as high as {@code IDLE_TIMEOUT + WATERMARK_INTERVAL}.
 */
public class AscendingTimestampsWatermarksWithIdleness<T> extends BoundedOutOfOrdernessWatermarks<T> {

	private static final long serialVersionUID = 1L;


	/**
	 * Creates a new watermark generator with the given idle timeout.
	 *
	 * @param idleTimeout The duration after which the generator marks its output as idle.
	 */
	public AscendingTimestampsWatermarksWithIdleness(Duration idleTimeout) {
		super(Duration.ZERO, idleTimeout);
	}
}
