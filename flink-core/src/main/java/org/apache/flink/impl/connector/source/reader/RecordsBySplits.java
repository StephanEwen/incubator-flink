/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.impl.connector.source.reader;

import org.apache.flink.api.connectors.source.SourceSplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of RecordsWithSplitIds to host all the records by splits.
 */
public class RecordsBySplits<E> implements RecordsWithSplitIds<E> {
	private Map<String, Collection<E>> recordsBySplits = new LinkedHashMap<>();
	private Set<String> finishedSplits = new HashSet<>();

	public void add(String splitId, E record) {
		recordsBySplits.computeIfAbsent(splitId, sid -> new ArrayList<>()).add(record);
	}

	public void add(SourceSplit sourceSplit, E record) {
		add(sourceSplit.splitId(), record);
	}

	public void addAll(String splitId, Collection<E> records) {
		this.recordsBySplits.compute(splitId, (id, r) -> {
			if (r == null) {
				r = records;
			} else {
				r.addAll(records);
			}
			return r;
		});
	}

	public void addAll(SourceSplit split, Collection<E> records) {
		addAll(split.splitId(), records);
	}

	public void addFinishedSplit(String splitId) {
		finishedSplits.add(splitId);
	}

	public void addFinishedSplits(Collection<String> splitIds) {
		finishedSplits.addAll(splitIds);
	}

	@Override
	public Set<String> finishedSplits() {
		return finishedSplits;
	}

	@Override
	public Collection<String> splitIds() {
		return recordsBySplits.keySet();
	}

	@Override
	public Map<String, Collection<E>> recordsBySplits() {
		return recordsBySplits;
	}
}
