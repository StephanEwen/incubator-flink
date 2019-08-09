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

package org.apache.flink.streaming.runtime.operators.exercise;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalKeyedTimerService;
import org.apache.flink.streaming.api.operators.InternalKeyedTimerServiceImpl;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.exercise.assigners.WindowAssigner;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.Trigger;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.TriggerResult;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The exercise Window Operator.
 */
@Internal
public class WindowOperator<K, IN, OUT>
	extends AbstractUdfStreamOperator<OUT, WindowFunction<IN, OUT, K, TimeWindow>>
	implements OneInputStreamOperator<IN, OUT>, Triggerable<K, TimeWindow> {

	private static final long serialVersionUID = 1L;

	static final TypeSerializer<TimeWindow> WINDOW_SERIALIZER = new TimeWindow.Serializer();

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	private final WindowAssigner<? super IN> windowAssigner;

	private transient WindowAssigner.WindowAssignerContext assignerContext;

	private final Trigger<? super IN> trigger;

	private transient TriggerContextImpl<K> triggerContext;

	private final StateDescriptor<? extends ListState<IN>, ?> windowStateDescriptor;

	private transient InternalListState<K, TimeWindow, IN> windowState;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public WindowOperator(
			WindowFunction<IN, OUT, K, TimeWindow> windowFunction,
			WindowAssigner<? super IN> windowAssigner,
			Trigger<? super IN> trigger,
			StateDescriptor<ListState<IN>, ?> windowStateDescriptor) {

		super(windowFunction);

		this.windowAssigner = checkNotNull(windowAssigner);
		this.trigger = checkNotNull(trigger);
		this.windowStateDescriptor = checkNotNull(windowStateDescriptor);
	}

	@Override
	public void open() throws Exception {
		super.open();

		windowState = (InternalListState<K, TimeWindow, IN>) getOrCreateKeyedState(WINDOW_SERIALIZER, windowStateDescriptor);

		final InternalTimerService<TimeWindow> ts = getInternalTimerService("window-timers", WINDOW_SERIALIZER, this);
		final InternalKeyedTimerService<K, TimeWindow> kts = new InternalKeyedTimerServiceImpl<>(ts, this);

		triggerContext = new TriggerContextImpl<>(kts);

		final ProcessingTimeService procService = getProcessingTimeService();
		assignerContext = procService::getCurrentProcessingTime;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final K key = this.<K>getKeyedStateBackend().getCurrentKey();
		final IN value = element.getValue();
		final long eventTimestamp = element.getTimestamp();

		final Collection<TimeWindow> windows = windowAssigner.assignWindows(value, eventTimestamp, assignerContext);

		for (TimeWindow window : windows) {
			windowState.setCurrentNamespace(window);
			windowState.add(value);
			triggerContext.set(key, window);
			final TriggerResult result = trigger.onElement(value, eventTimestamp, window, triggerContext);

			if (result.isFire()) {
				triggerWindow(key, window);
			}
			if (result.isPurge()) {
				windowState.setCurrentNamespace(window);
				windowState.clear();
				trigger.clear(window, triggerContext);
			}
		}
	}

	@Override
	public void onEventTime(InternalTimer<K, TimeWindow> timer) throws Exception {
		final K key = timer.getKey();
		final TimeWindow window = timer.getNamespace();
		triggerContext.set(key, window);
		final TriggerResult result = trigger.onEventTime(timer.getTimestamp(), window, triggerContext);

		if (result.isFire()) {
			triggerWindow(key, window);
		}
		if (result.isPurge()) {
			setCurrentKey(key);
			windowState.setCurrentNamespace(window);
			windowState.clear();
			trigger.clear(window, triggerContext);
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, TimeWindow> timer) throws Exception {
		K key = timer.getKey();
		TimeWindow window = timer.getNamespace();
		triggerContext.set(key, window);
		final TriggerResult result = trigger.onProcessingTime(timer.getTimestamp(), window, triggerContext);

		if (result.isFire()) {
			triggerWindow(key, window);
		}
		if (result.isPurge()) {
			setCurrentKey(key);
			windowState.setCurrentNamespace(window);
			windowState.clear();
			trigger.clear(window, triggerContext);
		}
	}

	private void triggerWindow(K key, TimeWindow window) throws Exception {
		setCurrentKey(key);
		windowState.setCurrentNamespace(window);
		final Iterable<IN> windowContents = windowState.get();
		if (windowContents == null) {
			return;
		}

		final TimestampedCollector<OUT> collector = new TimestampedCollector<>(output, window.maxTimestamp());
		userFunction.apply(key, window, windowContents, collector);
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
