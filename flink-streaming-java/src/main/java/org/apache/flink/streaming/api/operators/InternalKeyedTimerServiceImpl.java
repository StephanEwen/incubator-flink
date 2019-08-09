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

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * {@link InternalKeyedTimerService} that stores timers on the Java heap.
 */
public class InternalKeyedTimerServiceImpl<K, N> implements InternalKeyedTimerService<K, N> {

	private final InternalTimerService<N> timerService;

	private final KeyContext keyContext;

	public InternalKeyedTimerServiceImpl(
		KeyGroupRange localKeyGroupRange,
		KeyContext keyContext,
		ProcessingTimeService processingTimeService,
		KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue,
		KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue) {

		this(new InternalTimerServiceImpl<>(
					localKeyGroupRange,
					keyContext,
					processingTimeService,
					processingTimeTimersQueue,
					eventTimeTimersQueue),
			keyContext);
	}

	public InternalKeyedTimerServiceImpl(InternalTimerService<N> timerService, KeyContext keyContext) {
		this.timerService = timerService;
		this.keyContext = keyContext;
	}

	@Override
	public long currentProcessingTime() {
		return timerService.currentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return timerService.currentWatermark();
	}

	@Override
	public void registerProcessingTimeTimer(K key, N namespace, long time) {
		keyContext.setCurrentKey(key);
		timerService.registerProcessingTimeTimer(namespace, time);
	}

	@Override
	public void registerEventTimeTimer(K key, N namespace, long time) {
		keyContext.setCurrentKey(key);
		timerService.registerEventTimeTimer(namespace, time);
	}

	@Override
	public void deleteProcessingTimeTimer(K key, N namespace, long time) {
		keyContext.setCurrentKey(key);
		timerService.deleteProcessingTimeTimer(namespace, time);
	}

	@Override
	public void deleteEventTimeTimer(K key, N namespace, long time) {
		keyContext.setCurrentKey(key);
		timerService.deleteEventTimeTimer(namespace, time);
	}
}
