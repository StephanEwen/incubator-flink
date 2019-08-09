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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class TestInFlightWindowState<K, T> implements InFlightWindowState<K, T> {

	private final HashMap<Tuple2<K, TimeWindow>, List<T>> state = new HashMap<>();

	@Override
	public void add(K key, TimeWindow window, T element) throws Exception {
		final List<T> elements = state.computeIfAbsent(
			new Tuple2<>(key, window),
			(ignored) -> new ArrayList<>()
		);
		elements.add(element);
	}

	@Override
	public Iterable<T> get(K key, TimeWindow window) throws Exception {
		return state.get(new Tuple2<>(key, window));
	}

	@Override
	public void clear(K key, TimeWindow window) throws Exception {
		state.remove(new Tuple2<>(key, window));
	}

	public int getNumberOfKeyAndWindows() {
		return state.size();
	}
}
