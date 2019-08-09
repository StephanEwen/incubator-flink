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

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.exercise.assigners.WindowAssigner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;

/**
 *
 */
public class WindowAssignment<K, T, OUT>  {

	private final InFlightWindows<K, T, OUT> inFlightWindows;

	private final WindowTriggers<K, T> triggers;

	private final WindowAssigner<? super T> windowAssigner;

	private final WindowAssigner.WindowAssignerContext assignerContext;

	public WindowAssignment(
			WindowAssigner<? super T> windowAssigner,
			WindowAssigner.WindowAssignerContext assignerContext,
			InFlightWindows<K, T, OUT> inFlightWindows,
			WindowTriggers<K, T> triggers) {

		this.windowAssigner = windowAssigner;
		this.assignerContext = assignerContext;
		this.inFlightWindows = inFlightWindows;
		this.triggers = triggers;
	}


	public void onElement(K key, StreamRecord<T> element) throws Exception {
		final T value = element.getValue();
		final long eventTimestamp = element.getTimestamp();

		final Collection<TimeWindow> windows = windowAssigner.assignWindows(
			value, eventTimestamp, assignerContext);

		for (TimeWindow window : windows) {
			inFlightWindows.addElement(key, window, value);
			triggers.onElement(key, window, value, eventTimestamp);
		}
	}
}
