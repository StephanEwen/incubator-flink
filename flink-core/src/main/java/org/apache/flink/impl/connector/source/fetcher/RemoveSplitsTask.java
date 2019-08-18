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
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.splitreader.SplitsChange;
import org.apache.flink.impl.connector.source.splitreader.SplitsRemoval;

import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * The task to remove splits.
 */
class RemoveSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {
	private final SplitReader<?, SplitT> splitReader;
	private final List<SplitT> splitsToRemove;
	private final Queue<SplitsChange<SplitT>> splitsChanges;
	private final Map<String, SplitT> assignedSplits;
	private boolean splitsChangesAdded;

	RemoveSplitsTask(SplitReader<?, SplitT> splitReader,
					 List<SplitT> splitsToRemove,
					 Queue<SplitsChange<SplitT>> splitsChanges,
					 Map<String, SplitT> assignedSplits) {
		this.splitReader = splitReader;
		this.splitsToRemove = splitsToRemove;
		this.splitsChanges = splitsChanges;
		this.assignedSplits = assignedSplits;
		this.splitsChangesAdded = false;
	}

	@Override
	public boolean run() throws InterruptedException {
		if (!splitsChangesAdded) {
			splitsChanges.add(new SplitsRemoval<>(splitsToRemove));
			splitsToRemove.forEach(s -> assignedSplits.remove(s.splitId()));
			splitsChangesAdded = true;
		}
		splitReader.handleSplitsChanges(splitsChanges);
		return splitsChanges.isEmpty();
	}

	@Override
	public String toString() {
		return String.format("RemoveSplitTask: [%s]", splitsToRemove);
	}
}

