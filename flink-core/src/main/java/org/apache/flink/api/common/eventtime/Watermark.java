/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * A Watermark is a special event in a stream that holds a timestamp. A watermark signifies
 * that no events with a timestamp older or equal to the watermark's time will occur after the
 * water. A watermark with timestamp T indicates that event time has progressed to time T.
 *
 * <p>Watermarks are the progress indicators in the data streams. They are initially created
 * at the sources and propagate through the streams and operators.
 *
 * <p>In some cases a watermark is only a heuristic, meaning some events with a lower timestamp
 * may still follow. In that case, it is up to the logic of the operators to decide what to do
 * with the "late events". Operators can for example ignore these late events, route them to a
 * different stream, or send update to their previously emitted results.
 *
 * <p>When a source reaches the end of the input, it emits a final watermark with timestamp
 * {@code Long.MAX_VALUE}, indicating the "end of time".
 */
@Public
public final class Watermark implements Serializable {

	private static final long serialVersionUID = 1L;

	/** The watermark that signifies end-of-event-time. */
	public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);

	// ------------------------------------------------------------------------

	/** The timestamp of the watermark in milliseconds. */
	private final long timestamp;

	/**
	 * Creates a new watermark with the given timestamp in milliseconds.
	 */
	public Watermark(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Returns the timestamp associated with this {@link Watermark} in milliseconds.
	 */
	public long getTimestamp() {
		return timestamp;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		return this == o ||
				o != null &&
				o.getClass() == Watermark.class &&
				((Watermark) o).timestamp == this.timestamp;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(timestamp);
	}

	@Override
	public String toString() {
		return "Watermark @ " + timestamp;
	}
}
