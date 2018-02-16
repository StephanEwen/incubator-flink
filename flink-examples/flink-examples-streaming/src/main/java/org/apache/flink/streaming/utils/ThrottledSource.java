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

package org.apache.flink.streaming.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A streaming source (non parallel) that emits data from an iterator in
 * a rate controllable manner.
 *
 * @param <T> The type of data events emitted.
 */
public class ThrottledSource<T> implements SourceFunction<T>, ResultTypeQueryable<T> {

	private static final long serialVersionUID = 1L;

	private final Class<T> type;

	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	private final Iterator<T> generator;

	private final int maxRecordsPerSecond;

	private volatile boolean running;

	public ThrottledSource(Iterator<T> generator, Class<T> type, int maxRecordsPerSecond) {
		checkArgument(generator instanceof Serializable, "The iterator must be non-null and java.io.Serializable");

		checkArgument(maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
				"maxRecordsPerSecond must be positive or -1 (infinite)");

		this.type = checkNotNull(type);
		this.generator = generator;
		this.maxRecordsPerSecond = maxRecordsPerSecond;
		this.running = true;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		// compute the throttling parameters
		final long nanosPerBatch;
		final int throttleBatchSize;

		if (maxRecordsPerSecond == -1) {
			// unlimited speed
			throttleBatchSize = -1;
			nanosPerBatch = 0;
		}
		else {
			if (maxRecordsPerSecond >= 10000) {
				// high rates: all throttling in intervals of 2ms
				nanosPerBatch = 2_000_000L;
				throttleBatchSize = maxRecordsPerSecond / 500;
			}
			else {
				throttleBatchSize = maxRecordsPerSecond / 20 + 1;
				nanosPerBatch = 1_000_000_000L / maxRecordsPerSecond * throttleBatchSize;
			}
		}

		long endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
		int currentBatch = 0;

		while (running && generator.hasNext()) {
			// emit the next random event
			T next = generator.next();
			if (next != null) {
				ctx.collect(next);
			}

			// throttling logic
			if (throttleBatchSize > 0 && ++currentBatch == throttleBatchSize) {
				currentBatch = 0;

				final long now = System.nanoTime();
				final int millisRemaining = (int) ((endOfNextBatchNanos - now) / 1_000_000);

				if (millisRemaining > 0) {
					endOfNextBatchNanos += nanosPerBatch;
					Thread.sleep(millisRemaining);
				}
				else {
					endOfNextBatchNanos = now + nanosPerBatch;
				}
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return TypeInformation.of(type);
	}
}
