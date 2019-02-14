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

package org.apache.flink.api.common.src.multisplit;

import org.apache.flink.annotation.Public;

import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A collection of records, accessible in sub-collections by split.
 */
@Public
@FunctionalInterface
public interface RecordsBySplit<RecordT, SplitT> {

	/**
	 * Gets the records for the given split.
	 */
	Iterable<RecordT> recordsForSplit(SplitT split);

	// ------------------------------------------------------------------------

	/**
	 * Creates a RecordsBatchBySplit for records from multiple source splits.
	 */
	static <RecordT, SplitT> RecordsBySplit<RecordT, SplitT> forMultipleSplits(
			Map<SplitT, Iterable<RecordT>> splitToRecords) {

		checkNotNull(splitToRecords);
		return splitToRecords::get;
	}

	/**
	 * Creates a RecordsBatchBySplit for records from multiple source splits.
	 */
	static <RecordT, SplitT, BatchT> RecordsBySplit<RecordT, SplitT> forMultipleSplits(
			BatchT records,
			BiFunction<BatchT, SplitT, Iterable<RecordT>> perSplitGetter) {

		checkNotNull(records);
		checkNotNull(perSplitGetter);
		return (split) -> perSplitGetter.apply(records, split);
	}
}
