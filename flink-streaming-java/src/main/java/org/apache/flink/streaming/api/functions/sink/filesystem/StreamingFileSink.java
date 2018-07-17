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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.DateTimeBucketer;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Sink that emits its input elements to {@link FileSystem} files within buckets. This is
 * integrated with the checkpointing mechanism to provide exactly once semantics.
 *
 *
 * <p>When creating the sink a {@code basePath} must be specified. The base directory contains
 * one directory for every bucket. The bucket directories themselves contain several part files,
 * with at least one for each parallel subtask of the sink which is writing data to that bucket.
 * These part files contain the actual output data.
 *
 *
 * <p>The sink uses a {@link Bucketer} to determine in which bucket directory each element should
 * be written to inside the base directory. The {@code Bucketer} can, for example, use time or
 * a property of the element to determine the bucket directory. The default {@code Bucketer} is a
 * {@link DateTimeBucketer} which will create one new bucket every hour. You can specify
 * a custom {@code Bucketer} using {@link #setBucketer(Bucketer)}.
 *
 *
 * <p>The filenames of the part files contain the part prefix, "part-", the parallel subtask index of the sink
 * and a rolling counter. For example the file {@code "part-1-17"} contains the data from
 * {@code subtask 1} of the sink and is the {@code 17th} bucket created by that subtask.
 * Part files roll based on the user-specified {@link RollingPolicy}. By default, a {@link DefaultRollingPolicy}
 * is used.
 *
 * <p>In some scenarios, the open buckets are required to change based on time. In these cases, the user
 * can specify a {@code bucketCheckInterval} (by default 1m) and the sink will check periodically and roll
 * the part file if the specified rolling policy says so.
 *
 * <p>Part files can be in one of three states: {@code in-progress}, {@code pending} or {@code finished}.
 * The reason for this is how the sink works together with the checkpointing mechanism to provide exactly-once
 * semantics and fault-tolerance. The part file that is currently being written to is {@code in-progress}. Once
 * a part file is closed for writing it becomes {@code pending}. When a checkpoint is successful the currently
 * pending files will be moved to {@code finished}.
 *
 *
 * <p>If case of a failure, and in order to guarantee exactly-once semantics, the sink should roll back to the state it
 * had when that last successful checkpoint occurred. To this end, when restoring, the restored files in {@code pending}
 * state are transferred into the {@code finished} state while any {@code in-progress} files are rolled back, so that
 * they do not contain data that arrived after the checkpoint from which we restore.
 *
 * <p><b>NOTE:</b>
 * <ol>
 *     <li>
 *         If checkpointing is not enabled the pending files will never be moved to the finished state.
 *     </li>
 *     <li>
 *         The part files are written using an instance of {@link Encoder}. By default, a
 *         {@link SimpleStringEncoder} is used, which writes the result of {@code toString()} for
 *         every element, separated by newlines. You can configure the writer using the
 *         {@link #setEncoder(Encoder)}.
 *     </li>
 * </ol>
 *
 * @param <IN> Type of the elements emitted by this sink
 */
@PublicEvolving
public class StreamingFileSink<IN, BucketID>
		extends RichSinkFunction<IN>
		implements CheckpointedFunction, CheckpointListener, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	// -------------------------- state descriptors ---------------------------

	private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
			new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
			new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

	// ------------------------ configuration fields --------------------------

	private final long bucketCheckInterval;

	// --------------------------- runtime fields -----------------------------

	private final Buckets<IN, ?> buckets;

	private transient ProcessingTimeService processingTimeService;

	//////////////////			State Related Fields			/////////////////////

	private transient ListState<byte[]> bucketStates;

	private transient ListState<Long> maxPartCountersState;

	/**
	 * Creates a new {@code StreamingFileSink} that writes files to the given base directory.
	 *
	 * <p>This uses a {@link DateTimeBucketer} as {@link Bucketer} and a {@link SimpleStringEncoder} as a writer.
	 */
	private StreamingFileSink(Buckets<IN, BucketID> buckets, long bucketCheckInterval) {
		this.buckets = Preconditions.checkNotNull(buckets);
		this.bucketCheckInterval = bucketCheckInterval;
	}

	public static <IN, BucketID> StreamingFileSink.BucketManagerBuilder<IN, BucketID> createBuilder(final Path basePath) {
		return new StreamingFileSink.BucketManagerBuilder<>(basePath);
	}

	/**
	 * A helper class that holds the configuration properties for the {@link DefaultRollingPolicy}.
	 */
	@PublicEvolving
	public static class BucketManagerBuilder<IN, BucketID> {

		private long bucketCheckInterval = 60L * 1000L;

		private final Path basePath;

		private BucketFactory<IN, BucketID> bucketFactory;

		private Bucketer<IN, BucketID> bucketer;

		private Encoder<IN> encoder;

		private RollingPolicy<BucketID> rollingPolicy;

		public BucketManagerBuilder(Path basePath) {
			this.basePath = Preconditions.checkNotNull(basePath);

			this.bucketFactory = new DefaultBucketFactory<>();
			this.bucketer = new DateTimeBucketer<>();
			this.encoder = new SimpleStringEncoder<>();
			this.rollingPolicy = DefaultRollingPolicy.create().build();
		}

		public StreamingFileSink.BucketManagerBuilder<IN, BucketID> withBucketFactory(BucketFactory<IN, BucketID> factory) {
			this.bucketFactory = Preconditions.checkNotNull(factory);
			return this;
		}

		public StreamingFileSink.BucketManagerBuilder<IN, BucketID> withBucketCheckInterval(long interval) {
			this.bucketCheckInterval = interval;
			return this;
		}

		public StreamingFileSink.BucketManagerBuilder<IN, BucketID> withEncoder(Encoder<IN> encoder) {
			this.encoder = Preconditions.checkNotNull(encoder);
			return this;
		}

		public StreamingFileSink.BucketManagerBuilder<IN, BucketID> withBucketer(Bucketer<IN, BucketID> bucketer) {
			this.bucketer = Preconditions.checkNotNull(bucketer);
			return this;
		}

		public StreamingFileSink.BucketManagerBuilder<IN, BucketID> withRollingPolicy(RollingPolicy<BucketID> policy) {
			this.rollingPolicy = Preconditions.checkNotNull(policy);
			return this;
		}

		/**
		 * Creates the actual sink.
		 */
		public StreamingFileSink<IN, BucketID> build() throws IOException {
			final Buckets<IN, BucketID> buckets =
					new Buckets<>(basePath, bucketer, bucketFactory, encoder, rollingPolicy);
			return new StreamingFileSink<>(buckets, bucketCheckInterval);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		buckets.setSubtaskIndex(subtaskIndex); // TODO: 7/17/18 ugly

		final OperatorStateStore stateStore = context.getOperatorStateStore();
		bucketStates = stateStore.getListState(BUCKET_STATE_DESC);
		maxPartCountersState = stateStore.getUnionListState(MAX_PART_COUNTER_STATE_DESC);

		if (context.isRestored()) {
			buckets.initializeState(bucketStates, maxPartCountersState);
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		buckets.publishUpToCheckpoint(checkpointId);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(bucketStates != null && maxPartCountersState != null, "sink has not been initialized");

		bucketStates.clear();
		maxPartCountersState.clear();

		buckets.snapshotState(
				context.getCheckpointId(),
				context.getCheckpointTimestamp(),
				bucketStates,
				maxPartCountersState);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
		long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
		processingTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		final long currentTime = processingTimeService.getCurrentProcessingTime();
		buckets.onProcessingTime(currentTime);
		processingTimeService.registerTimer(currentTime + bucketCheckInterval, this);
	}


	@Override
	public void invoke(IN value, Context context) throws Exception {
		buckets.onElement(value, context);
	}

	@Override
	public void close() throws Exception {
		buckets.close();
	}
}
