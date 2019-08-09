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
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.exercise.assigners.WindowAssigner;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.Trigger;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The exercise Window Operator.
 */
@Internal
public class WindowOperator<K, IN, OUT>
	extends AbstractUdfStreamOperator<OUT, WindowFunction<IN, OUT, K, TimeWindow>>
	implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	static final TypeSerializer<TimeWindow> WINDOW_SERIALIZER = new TimeWindow.Serializer();

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	private final WindowAssigner<? super IN> windowAssigner;

	private final Trigger<? super IN> trigger;

	private final StateDescriptor<? extends ListState<IN>, ?> windowStateDescriptor;

	// ------------------------------------------------------------------------
	// Runtime values
	// ------------------------------------------------------------------------

	private transient WindowAssignment<K, IN, OUT> windowAssignment;

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

		final InFlightWindows<K, IN, OUT> windows = createInFlightWindowsStore();
		final WindowTriggers<K, IN> triggers = createTriggers(windows);

		windowAssignment = createWindowAssignment(windows, triggers);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		windowAssignment.onElement(currentKey(), element);
	}

	private K currentKey() {
		return this.<K>getKeyedStateBackend().getCurrentKey();
	}

	// -- setup / init

	private InFlightWindows<K, IN, OUT> createInFlightWindowsStore() throws Exception {
		final InternalListState<K, TimeWindow, IN> windowState =
			(InternalListState<K, TimeWindow, IN>) getOrCreateKeyedState(WINDOW_SERIALIZER, windowStateDescriptor);

		return new InFlightWindows<>(
			new InFlightWindowFlinkState<>(this, windowState),
			userFunction,
			output);
	}

	private WindowTriggers<K, IN> createTriggers(InFlightWindows<K, IN, ?> toTrigger) {
		final Function<Triggerable<K, TimeWindow>, InternalKeyedTimerService<K, TimeWindow>> timerServiceFactory =
			(triggerable) -> {
				InternalTimerService<TimeWindow> ts = getInternalTimerService("window-timers", WINDOW_SERIALIZER, triggerable);
				return new InternalKeyedTimerServiceImpl<>(ts, this);
			};

		return new WindowTriggers<>(trigger, toTrigger, timerServiceFactory);
	}

	private WindowAssignment<K, IN, OUT> createWindowAssignment(
			InFlightWindows<K, IN, OUT> windows,
			WindowTriggers<K, IN> triggers) {

		final ProcessingTimeService procService = getProcessingTimeService();
		final WindowAssigner.WindowAssignerContext context = procService::getCurrentProcessingTime;

		return new WindowAssignment<>(windowAssigner, context, windows, triggers);
	}
}
