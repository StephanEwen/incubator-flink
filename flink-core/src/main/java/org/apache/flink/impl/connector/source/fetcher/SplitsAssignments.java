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

package org.apache.flink.impl.connector.source.fetcher;

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.impl.connector.source.splitreader.SplitsAddition;
import org.apache.flink.impl.connector.source.splitreader.SplitsChangesWithEpoch;

import java.util.List;

/**
 * A class manages the splits changes overtime.
 */
class SplitsAssignments<SplitT extends SourceSplit> {
	private long epoch;
	private volatile SplitsChangesWithEpoch<SplitT> splitsChangesWithEpoch;

	SplitsAssignments() {
		// The epoch starts from 0;
		this.epoch = 0;
		this.splitsChangesWithEpoch = new SplitsChangesWithEpoch<>();
	}

	/**
	 * Get the current splits with epoch. We don't need to synchronize on this method
	 * because the splitsChangeWithEpoch is immutable and updated atomically.
	 *
	 * @return the current splits with the epoch.
	 */
	SplitsChangesWithEpoch<SplitT> currentSplitsWithEpoch() {
		return splitsChangesWithEpoch;
	}

	/**
	 * Add some splits to the existing assigned splits.
	 *
	 * @param splitsToAdd the splits to add.
	 */
	public void addSplits(List<SplitT> splitsToAdd) {
		splitsChangesWithEpoch =
			splitsChangesWithEpoch.newEpochWithSplitsChange(new SplitsAddition<>(splitsToAdd), epoch++);
	}

	/**
	 * Check whether there are assigned splits.
	 * @return true if there are assigned splits, false otherwise.
	 */
	public boolean isEmpty() {
		return splitsChangesWithEpoch.splits().isEmpty();
	}
}
