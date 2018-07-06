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

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A simple encoder that encodes elements one at a time into an output stream.
 *
 * <p>The encoder is expected to be stateless and not buffer elements or maintain
 * state in any auxiliary data structure. Each invocation of the write method
 * may potentially go to a different stream.
 *
 * <p>The encoder is not expected to me threadsafe and should not be used across
 * multiple threads.
 *
 * @param <T> The type of the elements encoded through this encoder.
 */
@PublicEvolving
public interface ElementWiseEncoder<T> {

	/**
	 * Encodes an element into the given stream.
	 *
	 * @param element The element to encode.
	 * @param out The stream to write to.
	 *
	 * @throws IOException Thrown if the encoding fails, or forwarded from the stream.
	 */
	void write(T element, OutputStream out) throws IOException;

	/**
	 * Writes the encoding format's header into the given stream.
	 *
	 * <p>For example for Avro encoders, this would write the Schema string to the stream.
	 *
	 * @param out The stream to write to.
	 * @throws IOException Forwarded from writing to the stream.
	 */
	void writeHeader(OutputStream out) throws IOException;
}
