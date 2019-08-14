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

package org.apache.flink.impl.connector.source;

import org.apache.flink.api.connectors.source.SourceOutput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;

/**
 * Emit a record to the downstream.
 *
 * @param <E> the type of the record emitted by the {@link SplitReader}
 * @param <T> the type of records that are eventually emitted to the {@link SourceOutput}.
 * @param <SplitStateT> the mutable type of split state.
 */
public interface RecordEmitter<E, T, SplitStateT> extends Configurable {

	/**
	 * Process and emit the records to the {@link SourceOutput}. A few recommendations to the implementation
	 * are following:
	 *
	 * <p>The
	 *
	 * @param records
	 * @param output
	 * @param splitState
	 */
	void emitRecord(E records, SourceOutput<T> output, SplitStateT splitState);

	@Override
	default void configure(Configuration config) {
		// by default do nothing.
	}
}
