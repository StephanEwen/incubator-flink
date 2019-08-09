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

import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 *
 */
public final class InFlightWindowFlinkState<K, T> implements InFlightWindowState<K, T> {

	private final KeyContext keyContext;
	private final InternalListState<K, TimeWindow, T> flinkState;

	public InFlightWindowFlinkState(KeyContext keyContext, InternalListState<K, TimeWindow, T> flinkState) {
		this.keyContext = keyContext;
		this.flinkState = flinkState;
	}

	@Override
	public void add(K key, TimeWindow window, T element) throws Exception {
		setupKeyAndWindow(key, window);
		flinkState.add(element);
	}

	@Override
	public Iterable<T> get(K key, TimeWindow window) throws Exception {
		setupKeyAndWindow(key, window);
		return flinkState.get();
	}

	@Override
	public void clear(K key, TimeWindow window) {
		setupKeyAndWindow(key, window);
		flinkState.clear();
	}

	private void setupKeyAndWindow(K key, TimeWindow window) {
		keyContext.setCurrentKey(key);
		flinkState.setCurrentNamespace(window);
	}
}
