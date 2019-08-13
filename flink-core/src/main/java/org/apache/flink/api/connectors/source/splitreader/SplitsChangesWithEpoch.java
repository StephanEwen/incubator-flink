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

package org.apache.flink.api.connectors.source.splitreader;


import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A container class to associated the assigned splits with an epoch. This allows the
 * user implementation to check whether a change needs to be made when fetch the records.
 *
 * <p>This class is made immutable so we don't need wo worry about the synchronization.
 *
 * @param <SplitT> the type of the splits.
 */
public class SplitsChangesWithEpoch<SplitT> {
	private final SortedMap<Long, SplitsChange<SplitT>> splits;
	private final long currentEpoch;

	public SplitsChangesWithEpoch() {
		this.splits = Collections.emptySortedMap();
		// The epoch of an empty splits changes starting from -1;
		this.currentEpoch = -1L;
	}

	private SplitsChangesWithEpoch(SortedMap<Long, SplitsChange<SplitT>> splits) {
		this.splits = splits;
		this.currentEpoch = splits.lastKey();
	}

	/**
	 * Get a new copy of {@link SplitsChangesWithEpoch} and add a new {@link SplitsChange} to it.
	 * @param splitsChange the new split change.
	 * @param epoch the epoch associated with the new change.
	 * @return a new copy of {@link SplitsChangesWithEpoch}
	 */
	public SplitsChangesWithEpoch<SplitT> newEpochWithSplitsChange(SplitsChange<SplitT> splitsChange, long epoch) {
		SortedMap<Long, SplitsChange<SplitT>> newSplits = new TreeMap<>(splits);
		newSplits.put(epoch, splitsChange);
		return new SplitsChangesWithEpoch<>(newSplits);
	}

	/**
	 * @return The assigned splits.
	 */
	public SortedMap<Long, SplitsChange<SplitT>> splits() {
		return Collections.unmodifiableSortedMap(splits);
	}

	/**
	 * The epoch of the assigned splits. The epoch helps users check the change on the
	 * split assignment without going through all the splits.
	 *
	 * @return the epoch of the assigned splits.
	 */
	public long currentEpoch() {
		return currentEpoch;
	}
}