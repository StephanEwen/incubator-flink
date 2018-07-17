/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The manager of the different active buckets in the sink.
 * This class is responsible for all bucket-related operations and the actual
 * {@link StreamingFileSink} is responsible for plugging in the functionality offered by
 * the {@code Buckets} to the lifecycle of the operator.
 *
 * @param <IN> The type of input elements.
 */
public class Buckets<IN> {

	private static final Logger LOG = LoggerFactory.getLogger(Buckets.class);

	// ------------------------ configuration fields --------------------------

	private final Path basePath;

	private final BucketFactory<IN> bucketFactory;

	private final Bucketer<IN> bucketer;

	private final Encoder<IN> encoder;

	private final RollingPolicy rollingPolicy;

	// --------------------------- runtime fields -----------------------------

	private final int subtaskIndex;

	private final BucketerContext bucketerContext;

	private final Map<String, Bucket<IN>> activeBuckets;

	private RecoverableWriter fileSystemWriter;

	private long initMaxPartCounter;

	private long maxPartCounterUsed;

	// --------------------------- State Related Fields -----------------------------

	private BucketStateSerializer bucketStateSerializer;

	/**
	 * A private constructor creating a new empty bucket manager.
	 *
	 * @param subtaskIndex The index of the current subtask.
	 * @param basePath The base path for our buckets.
	 * @param bucketer The {@link Bucketer} provided by the user.
	 * @param bucketFactory The {@link BucketFactory} to be used to create buckets.
	 * @param encoder The {@link Encoder} to be used when writing data.
	 * @param rollingPolicy The {@link RollingPolicy} as specified by the user.
	 * @throws IOException If something went wrong during accessing the underlying filesystem.
	 */
	private Buckets(
			int subtaskIndex,
			Path basePath,
			Bucketer<IN> bucketer,
			BucketFactory<IN> bucketFactory,
			Encoder<IN> encoder,
			RollingPolicy rollingPolicy) throws IOException {

		this.subtaskIndex = subtaskIndex;

		this.basePath = Preconditions.checkNotNull(basePath);
		this.bucketer = Preconditions.checkNotNull(bucketer);
		this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
		this.encoder = Preconditions.checkNotNull(encoder);
		this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);

		this.initMaxPartCounter = 0L;
		this.maxPartCounterUsed = 0L;

		this.activeBuckets = new HashMap<>();
		this.bucketerContext = new BucketerContext();

		this.fileSystemWriter = FileSystem.get(basePath.toUri()).createRecoverableWriter();
		this.bucketStateSerializer = new BucketStateSerializer(
				fileSystemWriter.getResumeRecoverableSerializer(),
				fileSystemWriter.getCommitRecoverableSerializer()
		);
	}

	/**
	 * Initializes the state after recovery from a failure.
	 * @param bucketStates the state holding recovered state about active buckets.
	 * @param partCounterState the state holding the max previously used part counters.
	 * @throws Exception
	 */
	private void initializeState(final ListState<byte[]> bucketStates, final ListState<Long> partCounterState) throws Exception {
		Preconditions.checkState(fileSystemWriter != null && bucketStateSerializer != null, "sink has not been initialized");

		// When resuming after a failure:
		// 1) we get the max part counter used before in order to make sure that we do not overwrite valid data
		// 2) we commit any pending files for previous checkpoints (previous to the last successful one)
		// 3) we resume writing to the previous in-progress file of each bucket, and
		// 4) if we receive multiple states for the same bucket, we merge them.

		// get the max counter
		long maxCounter = 0L;
		for (long partCounter: partCounterState.get()) {
			maxCounter = Math.max(partCounter, maxCounter);
		}
		initMaxPartCounter = maxCounter;

		// get the restored buckets
		for (byte[] recoveredState : bucketStates.get()) {
			final BucketState bucketState = SimpleVersionedSerialization.readVersionAndDeSerialize(
					bucketStateSerializer, recoveredState);

			final String bucketId = bucketState.getBucketId();

			LOG.info("Recovered bucket for {}", bucketId);

			final Bucket<IN> restoredBucket = bucketFactory.restoreBucket(
					fileSystemWriter,
					subtaskIndex,
					initMaxPartCounter,
					encoder,
					bucketState
			);

			final Bucket<IN> existingBucket = activeBuckets.get(bucketId);
			if (existingBucket == null) {
				activeBuckets.put(bucketId, restoredBucket);
			} else {
				existingBucket.merge(restoredBucket);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("{} idx {} restored state for bucket {}", getClass().getSimpleName(),
						subtaskIndex, assembleBucketPath(bucketId));
			}
		}
	}

	void publishUpToCheckpoint(long checkpointId) throws IOException {
		final Iterator<Map.Entry<String, Bucket<IN>>> activeBucketIt =
				activeBuckets.entrySet().iterator();

		while (activeBucketIt.hasNext()) {
			Bucket<IN> bucket = activeBucketIt.next().getValue();
			bucket.commitUpToCheckpoint(checkpointId);

			if (!bucket.isActive()) {
				// We've dealt with all the pending files and the writer for this bucket is not currently open.
				// Therefore this bucket is currently inactive and we can remove it from our state.
				activeBucketIt.remove();
			}
		}
	}

	void snapshotState(
			final long checkpointId,
			final long checkpointTimestamp,
			final ListState<byte[]> bucketStates,
			final ListState<Long> partCounterState) throws Exception {

		Preconditions.checkState(fileSystemWriter != null && bucketStateSerializer != null, "sink has not been initialized");

		for (Bucket<IN> bucket : activeBuckets.values()) {
			final PartFileInfo info = bucket.getInProgressPartInfo();

			if (info != null && rollingPolicy.shouldRoll(info, checkpointTimestamp)) {
				// we also check here so that we do not have to always
				// wait for the "next" element to arrive.
				bucket.closePartFile();
			}

			final BucketState bucketState = bucket.snapshot(checkpointId);
			bucketStates.add(SimpleVersionedSerialization.writeVersionAndSerialize(bucketStateSerializer, bucketState));
		}

		partCounterState.add(maxPartCounterUsed);
	}

	/**
	 * Called on every incoming element to write it to its final location.
	 * @param value the element itself.
	 * @param context the {@link SinkFunction.Context context} available to the sink function.
	 * @throws Exception
	 */
	void onElement(IN value, SinkFunction.Context context) throws Exception {
		final long currentProcessingTime = context.currentProcessingTime();

		// setting the values in the bucketer context
		bucketerContext.update(context.timestamp(), currentProcessingTime, context.currentWatermark());

		final String bucketId = bucketer.getBucketId(value, bucketerContext);

		Bucket<IN> bucket = activeBuckets.get(bucketId);
		if (bucket == null) {
			final Path bucketPath = assembleBucketPath(bucketId);
			bucket = bucketFactory.getNewBucket(
					fileSystemWriter,
					subtaskIndex,
					bucketId,
					bucketPath,
					initMaxPartCounter,
					encoder);
			activeBuckets.put(bucketId, bucket);
		}

		final PartFileInfo info = bucket.getInProgressPartInfo();
		if (info == null || rollingPolicy.shouldRoll(info, currentProcessingTime)) {
			bucket.rollPartFile(currentProcessingTime);
		}
		bucket.write(value, currentProcessingTime);

		// we update the counter here because as buckets become inactive and
		// get removed in the initializeState(), at the time we snapshot they
		// may not be there to take them into account during checkpointing.
		updateMaxPartCounter(bucket.getPartCounter());
	}

	void onProcessingTime(long timestamp) throws Exception {
		for (Bucket<IN> bucket : activeBuckets.values()) {
			final PartFileInfo info = bucket.getInProgressPartInfo();
			if (info != null && rollingPolicy.shouldRoll(info, timestamp)) {
				bucket.closePartFile();
			}
		}
	}

	void close() {
		if (activeBuckets != null) {
			activeBuckets.values().forEach(Bucket::dispose);
		}
	}

	/**
	 * Assembles the final bucket {@link Path} that will be used for the provided bucket in the
	 * underlying filesystem.
	 * @param bucketId the id of the bucket as returned by the {@link Bucketer}.
	 * @return The resulting path.
	 */
	private Path assembleBucketPath(String bucketId) {
		return new Path(basePath, bucketId);
	}

	/**
	 * Updates the state keeping track of the maximum used part
	 * counter across all local active buckets.
	 * @param candidate the part counter that will potentially replace the current {@link #maxPartCounterUsed}.
	 */
	private void updateMaxPartCounter(long candidate) {
		maxPartCounterUsed = Math.max(maxPartCounterUsed, candidate);
	}

	/**
	 * The {@link Bucketer.Context} exposed to the
	 * {@link Bucketer#getBucketId(Object, Bucketer.Context)}
	 * whenever a new incoming element arrives.
	 */
	private static class BucketerContext implements Bucketer.Context {

		@Nullable
		private Long elementTimestamp;

		private long currentWatermark;

		private long currentProcessingTime;

		void update(@Nullable Long element, long watermark, long processingTime) {
			this.elementTimestamp = element;
			this.currentWatermark = watermark;
			this.currentProcessingTime = processingTime;
		}

		@Override
		public long currentProcessingTime() {
			return currentProcessingTime;
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		@Nullable
		public Long timestamp() {
			return elementTimestamp;
		}
	}

	// ------------------------ Factory Methods to create instances --------------------------

	/**
	 * Create a new/fresh {@link Buckets bucket manager}.
	 *
	 * @param subtaskIdx the index of the current subtask.
	 * @param path the base path for our buckets.
	 * @param bucketer the {@link Bucketer} provided by the user.
	 * @param bucketFactory the {@link BucketFactory} to be used to create buckets.
	 * @param encoder the {@link Encoder} to be used when writing data.
	 * @param rollingPolicy the {@link RollingPolicy} as specified by the user.
	 * @param <INPUT> the type of input elements.
	 * @return The resulting {@code Bucket Manager}.
	 * @throws IOException If something went wrong either during accessing the underlying filesystem.

	 */
	public static <INPUT> Buckets<INPUT> init(
			final int subtaskIdx,
			final Path path,
			final Bucketer<INPUT> bucketer,
			final BucketFactory<INPUT> bucketFactory,
			final Encoder<INPUT> encoder,
			final RollingPolicy rollingPolicy) throws IOException {

		return new Buckets<>(subtaskIdx, path, bucketer, bucketFactory, encoder, rollingPolicy);
	}

	/**
	 * Restore the {@link Buckets bucket manager} after a failure or after a savepoint.
	 *
	 * @param subtaskIdx the index of the current subtask.
	 * @param path the base path for our buckets.
	 * @param bucketer the {@link Bucketer} provided by the user.
	 * @param bucketFactory the {@link BucketFactory} to be used to create buckets.
	 * @param encoder the {@link Encoder} to be used when writing data.
	 * @param rollingPolicy the {@link RollingPolicy} as specified by the user.
	 * @param bucketStates the restored state to initialize the active buckets from.
	 * @param partCounterState the restored state to initialize the part counter from.
	 * @param <INPUT> the type of input elements.
	 * @return The resulting {@code Bucket Manager}.
	 * @throws Exception If something went wrong either during state reading or during accessing the underlying filesystem.
	 */
	public static <INPUT> Buckets<INPUT> restore(
			final int subtaskIdx,
			final Path path,
			final Bucketer<INPUT> bucketer,
			final BucketFactory<INPUT> bucketFactory,
			final Encoder<INPUT> encoder,
			final RollingPolicy rollingPolicy,
			final ListState<byte[]> bucketStates,
			final ListState<Long> partCounterState) throws Exception {

		final Buckets<INPUT> buckets =
				new Buckets<>(subtaskIdx, path, bucketer, bucketFactory, encoder, rollingPolicy);
		buckets.initializeState(bucketStates, partCounterState);
		return buckets;
	}
}
