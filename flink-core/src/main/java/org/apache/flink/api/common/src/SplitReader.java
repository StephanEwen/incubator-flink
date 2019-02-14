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

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

/**
 * A reader that pulls records from a single source split.
 *
 * <p>For efficiency, the SplitReader should typically poll a set of records together,
 * rather than record by record.
 *
 * <p>For cases where a single reader polls from multiple shards at the same time, use the
 * {@link org.apache.flink.api.common.src.multisplit.MultiSplitReader}.
 *
 * @param <RecordsT> The type of the records that the split reader pulls from the split.
 */
public interface SplitReader<RecordsT> extends Closeable {

	@Nullable
	RecordsT fetchNextRecords(Duration timeout) throws IOException;

	void wakeup();

	@Override
	void close() throws IOException;
}
