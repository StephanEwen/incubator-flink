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

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A watermark generator that assumes monotonically ascending timestamps within the
 * stream split and periodically generates watermarks based on that assumption.
 * In addition, if no event is observed within a certain time, the generator marks its
 * output as {@link WatermarkOutput#markIdle() idle}.
 *
 * <p>Idleness is determined as part of the periodic watermark generation. Hence the time it
 * takes for the output to be marked as idle may be as high as {@code IDLE_TIMEOUT + WATERMARK_INTERVAL}.
 */
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

	private static final long serialVersionUID = 1L;

	/** The maximum timestamp encountered so far. */
	private long maxTimestamp = Long.MIN_VALUE;

	/** Counter to detect change. No problem if it overflows. */
	private long counter;

	/** The value of the counter at the last periodic watermark probe. */
	private long lastCounter;

	/** The first time (relative to {@link System#nanoTime()}) when the periodic watermark probe
	 * found that no event was processed since the last probe.
	 * Special values: 0 = no timer, LONG_MAX = timer already expired. */
	private long startOfInactivityNanos;

	/** The duration before the output is marked as idle. */
	private final long outOfOrdernessMillis;

	/** The duration before the output is marked as idle. */
	private final long maxIdleTimeNanos;

	/**
	 * Creates a new watermark generator with the given out-of-orderness bound.
	 *
	 * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
	 */
	public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
		this(maxOutOfOrderness, Duration.ofSeconds(Long.MAX_VALUE));
	}

	/**
	 * Creates a new watermark generator with the given out-of-orderness bound and
	 * an additional idle timeout.
	 *
	 * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
	 * @param idleTimeout The duration after which the generator marks its output as idle.
	 */
	public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness, Duration idleTimeout) {
		checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
		checkNotNull(idleTimeout, "idleTimeout");

		checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
		checkArgument(!idleTimeout.isNegative(), "idleTimeout cannot be negative");

		this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
		this.maxIdleTimeNanos = idleTimeout.toNanos();
	}

	// ------------------------------------------------------------------------

	@Override
	public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
		maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
		counter++; // record that something happened
	}

	@Override
	public void onPeriodicEmit(WatermarkOutput output) {
		final long lastCounterTmp = lastCounter;
		this.lastCounter = counter;

		if (counter != lastCounterTmp) {
			// activity since the last periodic probe
			// we emit a watermark and reset the timer
			startOfInactivityNanos = 0L;
			output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
		}
		else if (startOfInactivityNanos == 0L) {
			// first time that we see no activity since the last periodic probe
			// begin the timer
			startOfInactivityNanos = System.nanoTime();
		}
		else if (startOfInactivityNanos != Long.MAX_VALUE &&
				System.nanoTime() - startOfInactivityNanos > maxIdleTimeNanos) {

			startOfInactivityNanos = Long.MAX_VALUE;
			output.markIdle();
		}
	}
}
