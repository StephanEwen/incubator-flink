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

package org.apache.flink.streaming.runtime.operators.exercise;

import org.apache.flink.streaming.api.operators.InternalKeyedTimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.Trigger;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.TriggerResult;

import java.util.function.Function;

/**
 *
 */
final class WindowTriggers<K, T> implements Triggerable<K, TimeWindow> {

	private final Trigger<? super T> trigger;

	private final TriggerTarget<K> target;

	private final TriggerContextImpl<K> context;

	public WindowTriggers(
			Trigger<? super T> trigger,
			TriggerTarget<K> target,
			Function<Triggerable<K, TimeWindow>, InternalKeyedTimerService<K, TimeWindow>> timerServiceFactory) {

		this.trigger = trigger;
		this.target = target;

		final InternalKeyedTimerService<K, TimeWindow> timers = timerServiceFactory.apply(this);
		context = new TriggerContextImpl<>(timers);
	}

	void onElement(K key, TimeWindow window, T element, long elementTimestamp) throws Exception {
		final Trigger.TriggerContext context = getContext(key, window);
		final TriggerResult result = trigger.onElement(element, elementTimestamp, window, context);
		handleTriggerResult(result, key, window, context);
	}

	void onEventTime(K key, TimeWindow window, long eventTime) throws Exception {
		final Trigger.TriggerContext context = getContext(key, window);
		final TriggerResult result = trigger.onEventTime(eventTime, window, context);
		handleTriggerResult(result, key, window, context);
	}

	void onProcessingTime(K key, TimeWindow window, long processingTime) throws Exception {
		final Trigger.TriggerContext context = getContext(key, window);
		final TriggerResult result = trigger.onProcessingTime(processingTime, window, context);
		handleTriggerResult(result, key, window, context);
	}

	@Override
	public void onEventTime(InternalTimer<K, TimeWindow> timer) throws Exception {
		onEventTime(timer.getKey(), timer.getNamespace(), timer.getTimestamp());
	}

	@Override
	public void onProcessingTime(InternalTimer<K, TimeWindow> timer) throws Exception {
		onProcessingTime(timer.getKey(), timer.getNamespace(), timer.getTimestamp());
	}

	private void handleTriggerResult(
			TriggerResult result,
			K key,
			TimeWindow window,
			Trigger.TriggerContext context) throws Exception {

		if (result.isFire()) {
			target.triggerWindow(key, window);
		}
		if (result.isPurge()) {
			target.purgeWindow(key, window);
			trigger.clear(window, context);
		}
	}

	private Trigger.TriggerContext getContext(K key, TimeWindow window) {
		context.set(key, window);
		return context;
	}

	// ------------------------------------------------------------------------

	private static final class TriggerContextImpl<K> implements Trigger.TriggerContext {

		private final InternalKeyedTimerService<K, TimeWindow> timerService;

		private K currentKey;
		private TimeWindow currentWindow;

		TriggerContextImpl(InternalKeyedTimerService<K, TimeWindow> timerService) {
			this.timerService = timerService;
		}

		void set(K currentKey, TimeWindow window) {
			this.currentKey = currentKey;
			this.currentWindow = window;
		}

		@Override
		public long getCurrentProcessingTime() {
			return timerService.currentProcessingTime();
		}

		@Override
		public long getCurrentWatermark() {
			return timerService.currentWatermark();
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			timerService.registerProcessingTimeTimer(currentKey, currentWindow, time);
		}

		@Override
		public void registerEventTimeTimer(long time) {
			timerService.registerEventTimeTimer(currentKey, currentWindow, time);
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			timerService.deleteProcessingTimeTimer(currentKey, currentWindow, time);
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			timerService.deleteEventTimeTimer(currentKey, currentWindow, time);
		}
	}
}
