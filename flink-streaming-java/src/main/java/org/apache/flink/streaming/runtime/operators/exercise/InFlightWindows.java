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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 *
 */
public class InFlightWindows<K, T, OUT> implements TriggerTarget<K> {

	private final InFlightWindowState<K, T> state;

	private final WindowFunction<T, OUT, K, TimeWindow> windowFunction;

	private final Collector<StreamRecord<OUT>> out;

	public InFlightWindows(
			InFlightWindowState<K, T> state,
			WindowFunction<T, OUT, K, TimeWindow> windowFunction,
			Collector<StreamRecord<OUT>> out) {

		this.state = state;
		this.windowFunction = windowFunction;
		this.out = out;
	}

	public void addElement(K key, TimeWindow window, T element) throws Exception {
		state.add(key, window, element);
	}

	@Override
	public void triggerWindow(K key, TimeWindow window) throws Exception {
		final Iterable<T> windowContents = state.get(key, window);
		if (windowContents == null) {
			return;
		}

		final TimestampedCollector<OUT> collector = new TimestampedCollector<>(out, window.maxTimestamp());
		windowFunction.apply(key, window, windowContents, collector);
	}

	@Override
	public void purgeWindow(K key, TimeWindow window) throws Exception {
		state.clear(key, window);
	}

	@VisibleForTesting
	public Iterable<T> getWindow(K key, TimeWindow window) throws Exception {
		return state.get(key, window);
	}
}
