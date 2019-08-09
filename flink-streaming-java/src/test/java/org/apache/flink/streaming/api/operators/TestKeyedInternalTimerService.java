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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nonnull;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Implementation of {@link InternalTimerService} meant to use for testing.
 */
@Internal
public class TestKeyedInternalTimerService<K, N> implements InternalKeyedTimerService<K, N> {

	private final Triggerable<K, N> target;

	private final PriorityQueue<Timer<K, N>> processingTimeTimersQueue;
	private final Set<Timer<K, N>> processingTimeTimers;

	private final Set<Timer<K, N>> watermarkTimers;
	private final PriorityQueue<Timer<K, N>> watermarkTimersQueue;

	private long currentProcessingTime = Long.MIN_VALUE;

	private long currentWatermark = Long.MIN_VALUE;

	public TestKeyedInternalTimerService() {
		this(new NoOpTriggerable<>());
	}

	public TestKeyedInternalTimerService(Triggerable<K, N> target) {
		this.target = target;

		watermarkTimers = new HashSet<>();
		watermarkTimersQueue = new PriorityQueue<>(100);
		processingTimeTimers = new HashSet<>();
		processingTimeTimersQueue = new PriorityQueue<>(100);
	}

	@Override
	public long currentProcessingTime() {
		return currentProcessingTime;
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	public void registerProcessingTimeTimer(K key, N namespace, long time) {
		Timer<K, N> timer = new Timer<>(time, key, namespace);
		if (processingTimeTimers.add(timer)) {
			processingTimeTimersQueue.add(timer);
		}
	}

	@Override
	public void registerEventTimeTimer(K key, N namespace, long time) {
		Timer<K, N> timer = new Timer<>(time, key, namespace);
		if (watermarkTimers.add(timer)) {
			watermarkTimersQueue.add(timer);
		}
	}

	@Override
	public void deleteProcessingTimeTimer(K key, N namespace, long time) {
		Timer<K, N> timer = new Timer<>(time, key, namespace);
		if (processingTimeTimers.remove(timer)) {
			processingTimeTimersQueue.remove(timer);
		}
	}

	@Override
	public void deleteEventTimeTimer(K key, N namespace, long time) {
		Timer<K, N> timer = new Timer<>(time, key, namespace);
		if (watermarkTimers.remove(timer)) {
			watermarkTimersQueue.remove(timer);
		}
	}

	public void advanceProcessingTime(long time) throws Exception {
		currentProcessingTime = time;

		Timer<K, N> timer;
		while ((timer = processingTimeTimersQueue.peek()) != null && timer.timestamp <= time) {
			processingTimeTimers.remove(timer);
			processingTimeTimersQueue.remove();

			target.onProcessingTime(timer);
		}
	}

	public void advanceWatermark(long time) throws Exception {
		currentWatermark = time;

		Timer<K, N> timer;
		while ((timer = watermarkTimersQueue.peek()) != null && timer.timestamp <= time) {
			watermarkTimers.remove(timer);
			watermarkTimersQueue.remove();

			target.onEventTime(timer);
		}
	}

	/**
	 * Internal class for keeping track of in-flight timers.
	 */
	private static class Timer<K, N> implements InternalTimer<K, N>, Comparable<Timer<K, N>> {

		private final long timestamp;
		private final K key;
		private final N namespace;

		public Timer(long timestamp, K key, N namespace) {
			this.timestamp = timestamp;
			this.key = key;
			this.namespace = namespace;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Nonnull
		public K getKey() {
			return key;
		}

		@Nonnull
		public N getNamespace() {
			return namespace;
		}

		@Override
		public int compareTo(Timer<K, N> o) {
			return Long.compare(this.timestamp, o.timestamp);
		}

		@Override
		public int comparePriorityTo(@Nonnull InternalTimer<?, ?> other) {
			return Long.compare(this.timestamp, other.getTimestamp());
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}

			Timer<?, ?> timer = (Timer<?, ?>) o;

			return timestamp == timer.timestamp
					&& key.equals(timer.key)
					&& namespace.equals(timer.namespace);

		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + key.hashCode();
			result = 31 * result + namespace.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Timer{" +
					"timestamp=" + timestamp +
					", key=" + key +
					", namespace=" + namespace +
					'}';
		}
	}

	public int numProcessingTimeTimers() {
		return processingTimeTimers.size();
	}

	public int numEventTimeTimers() {
		return watermarkTimers.size();
	}

	public int numProcessingTimeTimers(N namespace) {
		int count = 0;
		for (Timer<K, N> timer : processingTimeTimers) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}

		return count;
	}

	public int numEventTimeTimers(N namespace) {
		int count = 0;
		for (Timer<K, N> timer : watermarkTimers) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}

		return count;
	}

}
