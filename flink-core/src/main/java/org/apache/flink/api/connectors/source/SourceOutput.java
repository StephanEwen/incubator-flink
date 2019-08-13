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

package org.apache.flink.api.connectors.source;

/**
 * The collector is the interface provided by Flink task to the {@link SourceReader} to emit records
 * to downstream operators for message processing.
 */
public interface SourceOutput<T> {

	/**
	 * Emit an element without a timestamp. Equivalaent to {@link #collect(Object, Long) collect(timestamp, null)};
	 *
	 * @param element
	 */
	void collect(T element);

	/**
	 * Emit an element with timestamp.
	 *
	 * @param element
	 * @param timestamp
	 */
	void collect(T element, Long timestamp);
}
