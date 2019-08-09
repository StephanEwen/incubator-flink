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

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.TestKeyedInternalTimerService;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.exercise.assigners.WindowAssigner;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.Trigger;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link WindowAssignment}.
 */
@SuppressWarnings("SerializableStoresNonSerializable")
public class WindowOperatorTest {

	@Test
	public void testEmittedDataHasEndOfWindowTimestamp() throws Exception {
		// setup
		final ArrayList<StreamRecord<String>> result = new ArrayList<>();
		final InFlightWindows<String, String, String> windows = createInFlightWindows(result);

		final TimeWindow window = new TimeWindow(100, 200);
		windows.addElement("A", window, "element1");

		// test
		windows.triggerWindow("A", window);

		// assert
		result.forEach((record) -> assertEquals(window.maxTimestamp(), record.getTimestamp()));
	}

	@Test
	public void testAssignersToMultipleWindows() throws Exception {
		final TimeWindow w1 = new TimeWindow(100, 200);
		final TimeWindow w2 = new TimeWindow(200, 300);
		final WindowAssigner<String> assigner = (element, timestamp, context) -> Arrays.asList(w1, w2);

		final TestInFlightWindowState<String, String> windows = new TestInFlightWindowState<>();
		final WindowAssignment<String, String, String> operator = createWindowAssignment(assigner, new NeverTrigger<>(), windows);

		// test
		operator.onElement("A", new StreamRecord<>("value", 0L));

		// assert
		assertEquals(2, windows.getNumberOfKeyAndWindows());
		assertSingleElementWindow("value", windows.get("A", w1));
		assertSingleElementWindow("value", windows.get("A", w2));
	}

	@Test
	public void testAssignersToZeroWindows() throws Exception {
		final WindowAssigner<String> emptyAssigner = (element, timestamp, context) -> Collections.emptyList();
		final TestInFlightWindowState<String, String> windows = new TestInFlightWindowState<>();

		final WindowAssignment<String, String, String> operator =
			createWindowAssignment(emptyAssigner, new NeverTrigger<>(), windows);

		// test
		operator.onElement("A", new StreamRecord<>("value", 0L));

		// assert
		assertEquals(0, windows.getNumberOfKeyAndWindows());
	}

	@Test
	public void deleteTimerWhenWindowPurged() throws Exception {
		final TimeWindow window = new TimeWindow(20, 30);

		final TestKeyedInternalTimerService<String, TimeWindow> timerService = new TestKeyedInternalTimerService<>();
		timerService.registerEventTimeTimer("A", window, window.getEnd());
		timerService.registerEventTimeTimer("B", window, window.getEnd());

		final WindowTriggers<String, String> triggers = new WindowTriggers<>(
			new AlwaysPurgeTrigger<>(),
			new NoOpTriggerTarget<>(),
			(ignored) -> timerService);

		// test
		triggers.onElement("A", window, "element", 0L);

		// assert
		assertEquals(1, timerService.numEventTimeTimers());
	}

	// ------------------------------------------------------------------------
	//  assertion helpers
	// ------------------------------------------------------------------------

	private static <T> void assertSingleElementWindow(T element, Iterable<T> elements) {
		final Iterator<T> iter = elements.iterator();
		assertTrue("window has no elements", iter.hasNext());
		assertEquals("window has wrong element", element, iter.next());
		assertFalse("window has more than one element", iter.hasNext());
	}

	// ------------------------------------------------------------------------
	//  setup helpers
	// ------------------------------------------------------------------------

	private static <K, T> InFlightWindows<K, T, T> createInFlightWindows(List<StreamRecord<T>> result) {
		return new InFlightWindows<>(
			new TestInFlightWindowState<>(),
			passThroughWindowFn(),
			new ListCollector<>(result));
	}

	private static <K, T> InFlightWindows<K, T, T> createInFlightWindows(InFlightWindowState<K, T> stateStore) {
		return new InFlightWindows<>(
			stateStore,
			passThroughWindowFn(),
			new ListCollector<>(new ArrayList<>()));
	}

	private static <K, T> WindowTriggers<K, T> createTriggers(
			Trigger<? super T> trigger,
			TriggerTarget<K> target) {

		final TestKeyedInternalTimerService<K, TimeWindow> timerService = new TestKeyedInternalTimerService<>();

		return new WindowTriggers<>(
			trigger,
			target,
			(ignored) -> timerService);
	}

	private static <K, T> WindowAssignment<K, T, T> createWindowAssignment(
			WindowAssigner<? super T> assigner,
			Trigger<? super T> trigger,
			InFlightWindowState<K, T> stateStore) {

		final InFlightWindows<K, T, T> windows = createInFlightWindows(stateStore);
		final WindowTriggers<K, T> triggers = createTriggers(trigger, windows);

		return new WindowAssignment<>(assigner, assignerContext(), windows, triggers);

	}

	private static <K, T> WindowFunction<T, T, K, TimeWindow> passThroughWindowFn() {
		return (key, window, in, out) -> in.forEach(out::collect);
	}

	private static WindowAssigner.WindowAssignerContext assignerContext() {
		return System::currentTimeMillis;
	}
}
