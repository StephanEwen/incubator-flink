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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.ProductSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Either;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Configuration snapshot for serializers of the {@link Either} type,
 * containing configuration snapshots of the Left and Right serializers.
 */
@Internal
public final class EitherSerializerConfigSnapshot<L, R> implements TypeSerializerSnapshot<Either<L, R>> {

	private static final int VERSION = 2;

	@Nullable
	private ProductSerializerSnapshot nestedSerializers;

	/**
	 * This empty nullary constructor is required for deserializing the configuration.
	 */
	@SuppressWarnings("unused")
	public EitherSerializerConfigSnapshot() {}

	/**
	 * Constructor to create the snaphshot when writing it out.
	 */
	public EitherSerializerConfigSnapshot(
			TypeSerializer<L> leftSerializer,
			TypeSerializer<R> rightSerializer) {

		nestedSerializers = new ProductSerializerSnapshot(leftSerializer, rightSerializer);
	}

	// ------------------------------------------------------------------------

	@Override
	public int getCurrentVersion() {
		return VERSION;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		checkState(nestedSerializers != null);
		nestedSerializers.writeProductSnapshots(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
		switch (readVersion) {
			case 1:
				nestedSerializers = ProductSerializerSnapshot.legacyReadProductSnapshots(in, classLoader);
				break;
			case 2:
				nestedSerializers = ProductSerializerSnapshot.readProductSnapshot(in, classLoader);
				break;
			default:
				throw new IllegalArgumentException("Unrecognized version: " + readVersion);
		}
	}

	@Override
	public TypeSerializer<Either<L, R>> restoreSerializer() {
		checkState(nestedSerializers != null);

		return new EitherSerializer<>(
				nestedSerializers.getRestoreSerializer(0),
				nestedSerializers.getRestoreSerializer(1));
	}

	@Override
	public TypeSerializerSchemaCompatibility<Either<L, R>> resolveSchemaCompatibility(
			TypeSerializer<Either<L, R>> newSerializer) {

		checkState(nestedSerializers != null);

		return nestedSerializers.resolveSchemaCompatibility(
				newSerializer,
				EitherSerializer.class,
				(s) -> new TypeSerializer<?>[] { s.getLeftSerializer(), s.getRightSerializer() },
				(s) -> TypeSerializerSchemaCompatibility.compatibleAsIs()
			);
	}
}
