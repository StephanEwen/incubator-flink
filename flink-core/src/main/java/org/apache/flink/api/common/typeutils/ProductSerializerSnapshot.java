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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A CompositeSerializerSnapshot represents the snapshots of multiple serializers that are used
 * by an outer serializer. Examples would be tuples, where the outer serializer is the tuple
 * format serializer, an the CompositeSerializerSnapshot holds the serializers for the
 * different tuple fields.
 *
 * <p>The CompositeSerializerSnapshot has its own versioning internally, it does not couple its
 * versioning to the versioning of the TypeSerializerSnapshot that builds on top of this class.
 * That way, the CompositeSerializerSnapshot and enclosing TypeSerializerSnapshot the can evolve
 * their formats independently.
 */
@Internal
public class ProductSerializerSnapshot {

	/** Magic number for integrity checks during deserialization. */
	private static final int MAGIC_NUMBER = 1333245;

	/** Current version of the new serialization format. */
	private static final int VERSION = 1;

	/** The snapshots from the serializer that make up this composition. */
	private final TypeSerializerSnapshot<?>[] nestedSnapshots;

	/**
	 * Constructor to create a snapshot for writing.
	 */
	public ProductSerializerSnapshot(TypeSerializer<?>... serializers) {
		this.nestedSnapshots = TypeSerializerUtils.snapshotBackwardsCompatible(serializers);
	}

	/**
	 * Constructor to create a snapshot during deserialization.
	 */
	private ProductSerializerSnapshot(TypeSerializerSnapshot<?>[] snapshots) {
		this.nestedSnapshots = snapshots;
	}

	// ------------------------------------------------------------------------
	//  Nested Serializers and Compatibility
	// ------------------------------------------------------------------------

	public TypeSerializer<?>[] getRestoreSerializers() {
		return snapshotsToRestoreSerializers(nestedSnapshots);
	}

	public <T> TypeSerializer<T> getRestoreSerializer(int pos) {
		checkArgument(pos < nestedSnapshots.length);

		@SuppressWarnings("unchecked")
		TypeSerializerSnapshot<T> snapshot = (TypeSerializerSnapshot<T>) nestedSnapshots[pos];

		return snapshot.restoreSerializer();
	}

	public <T, S> TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
			TypeSerializer<T> outerSerializer,
			Class<S> requiredOuterSerializerType,
			Function<S, TypeSerializer<?>[]> serializerAccessor,
			Function<S, TypeSerializerSchemaCompatibility<?>> outerCompatibility) {

		// class compatibility
		if (!requiredOuterSerializerType.isInstance(outerSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}

		// compatibility of the outer serializer's format

		@SuppressWarnings("unchecked")
		final S castedSerializer = (S) outerSerializer;
		final TypeSerializerSchemaCompatibility<?> outerResult = outerCompatibility.apply(castedSerializer);

		if (outerResult.isIncompatible()) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
		if (outerResult.isCompatibleAfterMigration()) {
			return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
		}

		// check nested serializers for compatibility
		final TypeSerializer<?>[] nestedSerializers = serializerAccessor.apply(castedSerializer);
		checkState(nestedSerializers.length == nestedSnapshots.length);

		for (int i = 0; i < nestedSnapshots.length; i++) {
			TypeSerializerSchemaCompatibility<?> compatibility =
					resolveCompatibility(nestedSerializers[i], nestedSnapshots[i]);

			if (compatibility.isIncompatible()) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}
			if (compatibility.isCompatibleAfterMigration()) {
				return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
			}
		}

		return TypeSerializerSchemaCompatibility.compatibleAsIs();
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	/**
	 * Writes the composite snapshot of all the contained serializers.
	 */
	public final void writeProductSnapshots(DataOutputView out) throws IOException {
		out.writeInt(MAGIC_NUMBER);
		out.writeInt(VERSION);

		out.writeInt(nestedSnapshots.length);
		for (TypeSerializerSnapshot<?> snap : nestedSnapshots) {
			TypeSerializerSnapshot.writeVersionedSnapshot(out, snap);
		}
	}

	/**
	 * Reads the composite snapshot of all the contained serializers.
	 */
	public static ProductSerializerSnapshot readProductSnapshot(DataInputView in, ClassLoader cl) throws IOException {
		final int magicNumber = in.readInt();
		if (magicNumber != MAGIC_NUMBER) {
			throw new IOException(String.format("Corrupt data, magic number mismatch. Expected %8x, found %8x",
					MAGIC_NUMBER, magicNumber));
		}

		final int version = in.readInt();
		if (version != VERSION) {
			throw new IOException("Unrecognized version: " + version);
		}

		final int numSnapshots = in.readInt();
		final TypeSerializerSnapshot<?>[] nestedSnapshots = new TypeSerializerSnapshot<?>[numSnapshots];

		for (int i = 0; i < numSnapshots; i++) {
			nestedSnapshots[i] = TypeSerializerSnapshot.readVersionedSnapshot(in, cl);
		}

		return new ProductSerializerSnapshot(nestedSnapshots);
	}

	public static ProductSerializerSnapshot legacyReadProductSnapshots(DataInputView in, ClassLoader cl) throws IOException {
		throw new UnsupportedOperationException("bäh!");
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Utility method to conjure up a new scope for the generic parameters.
	 */
	@SuppressWarnings("unchecked")
	private static <E> TypeSerializerSchemaCompatibility<E> resolveCompatibility(
			TypeSerializer<?> serializer,
			TypeSerializerSnapshot<?> snapshot) {

		TypeSerializer<E> typedSerializer = (TypeSerializer<E>) serializer;
		TypeSerializerSnapshot<E> typedSnapshot = (TypeSerializerSnapshot<E>) snapshot;

		return typedSnapshot.resolveSchemaCompatibility(typedSerializer);
	}

	private static TypeSerializer<?>[] snapshotsToRestoreSerializers(TypeSerializerSnapshot<?>... snapshots) {
		return Arrays.stream(snapshots)
				.map(TypeSerializerSnapshot::restoreSerializer)
				.toArray(TypeSerializer[]::new);
	}
}
