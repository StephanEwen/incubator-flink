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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;

/**
 *
 * @param <T>        The type of records produced by the source.
 * @param <SplitT>   The type of splits handled by the source.
 * @param <CoordChkT> The type of the enumerator checkpoints.
 */
public interface Source<T, SplitT extends SourceSplit, CoordChkT> extends Serializable {

	/**
	 * Creates a new reader to read data from the spits it gets assigned.
	 * The reader starts fresh and does not have any state to resume.
	 *
	 * @param config A flat config for this source operator.
	 * @param context the {@link SourceContext} that exposes some runtime primitives.
	 */
	SourceReader<T, SplitT> createReader(Configuration config, SourceContext context) throws IOException;

	/**
	 * Creates a new SplitEnumerator for this source, starting a new input.
	 *
	 * @param config the configuration for this operator.
	 */
	SplitEnumerator<SplitT, CoordChkT> createEnumerator(Configuration config) throws IOException;

	/**
	 * Restores an enumerator from a checkpoint.
	 *
	 * @param config the configuration of this operator.
	 */
	SplitEnumerator<SplitT, CoordChkT> restoreEumerator(Configuration config, CoordChkT checkpoint) throws IOException;

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
	SimpleVersionedSerializer<CoordChkT> getEnumeratorCheckpointSerializer();
}
