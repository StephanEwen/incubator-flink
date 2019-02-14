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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 *
 * @param <T>        The type of records produced by the source.
 * @param <SplitT>   The type of splits handled by the source.
 * @param <EnumChkT> The type of the enumerator checkpoints.
 */
public interface Source<T, SplitT extends SourceSplit, EnumChkT> extends Serializable {

	/**
	 * Checks whether the source supports the given boundedness.
	 *
	 * <p>Some sources might only support either continuous unbounded streams, or
	 * bounded streams.
	 */
	boolean supportsBoundedness(Boundedness boundedness);

	/**
	 * Creates a new reader to read data from the spits it gets assigned.
	 * The reader starts fresh and does not have any state to resume.
	 */
	SourceReader<T, SplitT> createReader(SourceContext ctx) throws IOException;

	/**
	 * Creates a new SplitEnumerator for this source, starting a new input.
	 */
	SplitEnumerator<SplitT, EnumChkT> createEnumerator(Boundedness mode) throws IOException;

	/**
	 * Restores an enumerator from a checkpoint.
	 */
	SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(Boundedness mode, EnumChkT checkpoint) throws IOException;

	// ------------------------------------------------------------------------
	//  serializers for the metadata
	// ------------------------------------------------------------------------

	/**
	 * Creates a serializer for the input splits. Splits are serialized when sending them
	 * from enumerator to reader, and when checkpointing the reader's current state.
	 */
	SimpleVersionedSerializer<SplitT> getSplitSerializer();

	/**
	 * Creates the serializer for the {@link SplitEnumerator} checkpoint.
	 * The serializer is used for the result of the {@link SplitEnumerator#snapshotState()}
	 * method.
	 */
	SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer();
}
