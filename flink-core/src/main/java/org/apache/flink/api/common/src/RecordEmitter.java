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

package org.apache.flink.api.common.src;

/**
 * A RecordEmitter takes a raw record coming from a source, deserializes it into
 * the proper event type and emits that event (possibly together with timestamps
 * or watermarks).
 *
 * @param <E> The type of the events emitted.
 * @param <RawT> The type of the raw records.
 * @param <SplitT> The type of the source split that the records come from.
 */
@FunctionalInterface
public interface RecordEmitter<E, RawT, SplitT extends SourceSplit> {

	/**
	 * Deserializes a raw record and emits it.
	 *
	 * @param rawRecord The raw record, as obtained from the source.
	 * @param output The source output to send the proper event record to.
	 * @param split The source split that the record came from.
	 *
	 * @throws Exception The emitter can forward exceptions. The source may treat such
	 *                   exceptions as reasons to fail the specific reading task and recover.
	 */
	void emitRecord(RawT rawRecord, SourceOutput<E> output, SplitT split) throws Exception;
}
