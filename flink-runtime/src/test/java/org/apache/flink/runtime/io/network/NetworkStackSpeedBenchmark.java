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

package org.apache.flink.runtime.io.network;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup.DummyTaskIOMetricGroup;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.flink.util.ExceptionUtils.suppressExceptions;

/**
 * A benchmark for the throughput / latency characteristics of the network stack.
 * 
 * <p>This benchmark is intended to be run manually, or as part of performance regression tests.
 * It is not run as part of automatic unit / integration tests, because it cannot give
 * meaningful results on volatile CI infrastructures.
 */
public class NetworkStackSpeedBenchmark extends TestLogger {

	private static final int BUFFER_SIZE = TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue();

	private static final int NUM_SLOTS_AND_THREADS = 1;

	private static final InetAddress LOCAL_ADDRESS;

	static {
		try {
			LOCAL_ADDRESS = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new Error(e);
		}
	}

	// ------------------------------------------------------------------------
	//  Setup
	// ------------------------------------------------------------------------

	private final IOManager ioManager = new IOManagerAsync();

	@After
	public void shutdown() {
		ioManager.shutdown();
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testLatency() throws Exception {

		final JobID jobId = new JobID();
		final IntermediateDataSetID dataSetID = new IntermediateDataSetID();
		final ResultPartitionID senderID = new ResultPartitionID();
		final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

		final NetworkEnvironment senderEnv = createNettyNetworkEnvironment(2048);
		final NetworkEnvironment receiverEnv = createNettyNetworkEnvironment(2048);

		try {
			senderEnv.start();
			receiverEnv.start();

			final TaskManagerLocation senderLocation = new TaskManagerLocation(
					ResourceID.generate(),
					LOCAL_ADDRESS,
					senderEnv.getConnectionManager().getDataPort());

			final ResultPartition sender = createResultPartition(jobId, senderID, senderEnv);
			final RecordWriter<LongValue> writer = new RecordWriter<>(sender); 

			final SingleInputGate receiverGate = createInputGate(
					jobId, dataSetID, senderID, executionAttemptID, senderLocation, receiverEnv);

			final LatencyMeasuringReceiver receiver = new LatencyMeasuringReceiver(receiverGate, 200);
			receiver.start();

			Thread.sleep(500);

			for (int i = 0; i < 200; i++) {
				writer.sendToTarget(new LongValue(System.nanoTime()), 0);
				Thread.sleep(5);
			}

			Thread.sleep(100);

			// stop and gather exceptions
			receiver.shutdown();
			receiver.sync();

			System.out.println(String.format("Latency (nanos): min=%,d, max=%,d, avg=%,d, avgNoExt=%,d", 
					receiver.getMinLatency(), receiver.getMaxLatency(),
					receiver.getAvgLatency(), receiver.getAvgNoExtremes()));
		}
		finally {
			suppressExceptions(senderEnv::shutdown);
			suppressExceptions(receiverEnv::shutdown);
		}
	}

	@Test
	public void testThroughput() throws Exception {

		final JobID jobId = new JobID();
		final IntermediateDataSetID dataSetID = new IntermediateDataSetID();
		final ResultPartitionID senderID = new ResultPartitionID();
		final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

		final NetworkEnvironment senderEnv = createNettyNetworkEnvironment(2048);
		final NetworkEnvironment receiverEnv = createNettyNetworkEnvironment(2048);

		try {
			senderEnv.start();
			receiverEnv.start();

			final TaskManagerLocation senderLocation = new TaskManagerLocation(
					ResourceID.generate(),
					LOCAL_ADDRESS,
					senderEnv.getConnectionManager().getDataPort());

			final ResultPartition sender = createResultPartition(jobId, senderID, senderEnv);
			final RecordWriter<LongValue> writer = new RecordWriter<>(sender);

			final SingleInputGate receiverGate = createInputGate(
					jobId, dataSetID, senderID, executionAttemptID, senderLocation, receiverEnv);

			final long numRecords = 40_000_000L;

			final LatencyMeasuringReceiver receiver = new LatencyMeasuringReceiver(receiverGate, numRecords);
			receiver.start();

			Thread.sleep(500);

			final LongValue value = new LongValue();

			final long startTs = System.nanoTime();

			for (long i = numRecords, t = 1000; i > 0 ; i--, t--) {
				if (t == 0) {
					value.setValue(System.nanoTime());
					t = 1000;
				} else {
					value.setValue(0);
				}
				writer.sendToTarget(value, 0);
			}

			// wait till the receiver got all events
			receiver.sync();

			final long endTs = System.nanoTime();
			final long durationMillis = (endTs - startTs) / 1_000_000;

			System.out.println(String.format("Throughput: %,d recs/msec", numRecords / durationMillis));

			System.out.println(String.format("Latency (nanos): min=%,d, max=%,d, avg=%,d, avgNoExt=%,d",
					receiver.getMinLatency(), receiver.getMaxLatency(),
					receiver.getAvgLatency(), receiver.getAvgNoExtremes()));
		}
		finally {
			suppressExceptions(senderEnv::shutdown);
			suppressExceptions(receiverEnv::shutdown);
		}
	}

	// ------------------------------------------------------------------------
	//  Receivers
	// ------------------------------------------------------------------------

	private static class LatencyMeasuringReceiver extends CheckedThread {

		private final SingleInputGate receiver;

		private final long numRecords;

		private long maxLatency;
		private long minLatency;
		private long sumLatency;
		private int numSamples;

		private volatile boolean running;

		LatencyMeasuringReceiver(SingleInputGate receiver, long numRecords) {
			this.receiver = receiver;
			this.numRecords = numRecords;
			this.running = true;

			maxLatency = Long.MIN_VALUE;
			minLatency = Long.MAX_VALUE;
		}

		@Override
		public void go() throws Exception {

			final MutableRecordReader<LongValue> reader = new MutableRecordReader<>(
					receiver, new String[] { EnvironmentInformation.getTemporaryFileDirectory() } );

			try {
				final LongValue value = new LongValue();
				long remaining = numRecords;

				while (running && remaining-- > 0 && reader.next(value)) {
					final long ts = value.getValue();
					if (ts != 0) {
						final long latencyNanos = System.nanoTime() - ts;

						maxLatency = Math.max(maxLatency, latencyNanos);
						minLatency = Math.min(minLatency, latencyNanos);
						sumLatency += latencyNanos;
						numSamples++;
					}

					Thread.sleep(1);

				}
			}
			catch (InterruptedException e) {
				if (running) {
					throw e;
				}
			}
		}

		public void shutdown() {
			running = false;
			interrupt();
		}

		public long getMaxLatency() {
			return maxLatency == Long.MIN_VALUE ? 0 : maxLatency;
		}

		public long getMinLatency() {
			return minLatency == Long.MAX_VALUE ? 0 : minLatency;
		}

		public long getAvgLatency() {
			return numSamples == 0 ? 0 : sumLatency / numSamples;
		}

		public long getAvgNoExtremes() {
			return (numSamples > 2) ? (sumLatency - maxLatency - minLatency) / (numSamples - 2) : 0;
		}
	}

	private static class FastReceiver extends CheckedThread {

		private final SingleInputGate receiver;

		private volatile boolean running;

		FastReceiver(SingleInputGate receiver) {
			this.receiver = receiver;
			this.running = true;
		}

		@Override
		public void go() throws Exception {
			while (running) {
				final BufferOrEvent next;
				try {
					next = receiver.getNextBufferOrEvent();
				}
				catch (InterruptedException e) {
					// restore interruption status, fall through the loop
					interrupt();
					continue;
				}

				if (next.isBuffer()) {
					next.getBuffer().recycleBuffer();
				}
			}
		}

		public void shutdown() {
			running = false;
			interrupt();
		}
	}

	// ------------------------------------------------------------------------
	//  Setup Utilities
	// ------------------------------------------------------------------------

	private NetworkEnvironment createNettyNetworkEnvironment(int bufferPoolSize) throws Exception {

		final NetworkBufferPool bufferPool = new NetworkBufferPool(bufferPoolSize, BUFFER_SIZE);

		final NettyConnectionManager nettyConnectionManager = new NettyConnectionManager(
				new NettyConfig(LOCAL_ADDRESS, 0, BUFFER_SIZE, NUM_SLOTS_AND_THREADS, new Configuration()));

		return new NetworkEnvironment(
				bufferPool,
				nettyConnectionManager,
				new ResultPartitionManager(),
				new TaskEventDispatcher(),
				new KvStateRegistry(),
				null,
				IOMode.SYNC,
				TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL.defaultValue(),
				TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX.defaultValue(),
				TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue(),
				TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue());
	}

	private ResultPartition createResultPartition(
			JobID jobId,
			ResultPartitionID partitionId,
			NetworkEnvironment env) throws Exception {

		ResultPartition partition = new ResultPartition(
				"sender task",
				new NoOpTaskActions(),
				jobId,
				partitionId,
				ResultPartitionType.PIPELINED_BOUNDED,
				1,
				1,
				env.getResultPartitionManager(),
				new NoOpResultPartitionConsumableNotifier(), 
				ioManager,
				false);

		int numBuffers = TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue() +
				TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue();

		BufferPool bufferPool = env.getNetworkBufferPool().createBufferPool(1, numBuffers);
		partition.registerBufferPool(bufferPool);

		env.getResultPartitionManager().registerResultPartition(partition);

		return partition;
	}

	private SingleInputGate createInputGate(
			JobID jobId,
			IntermediateDataSetID dataSetID,
			ResultPartitionID consumedPartitionId,
			ExecutionAttemptID executionAttemptID,
			TaskManagerLocation senderLocation,
			NetworkEnvironment env) throws IOException {

		final InputChannelDeploymentDescriptor channelDescr = new InputChannelDeploymentDescriptor(
				consumedPartitionId,
				ResultPartitionLocation.createRemote(new ConnectionID(senderLocation, 0)));

		final InputGateDeploymentDescriptor gateDescr = new InputGateDeploymentDescriptor(
				dataSetID,
				ResultPartitionType.PIPELINED_BOUNDED,
				0,
				new InputChannelDeploymentDescriptor[] { channelDescr } );

		SingleInputGate gate = SingleInputGate.create(
				"receiving task",
				jobId,
				executionAttemptID,
				gateDescr,
				env,
				new NoOpTaskActions(),
				new DummyTaskIOMetricGroup());

		int numBuffers = TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue() +
				TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue();

		BufferPool bufferPool =
				env.getNetworkBufferPool().createBufferPool(1, numBuffers);

		gate.setBufferPool(bufferPool);

		return gate;
	}

	// ------------------------------------------------------------------------
	//  Mocks
	// ------------------------------------------------------------------------

	/**
	 * A dummy implementation of the {@link TaskActions}. We implement this here rather than using Mockito
	 * to avoid using mockito in this benchmark class.
	 */
	private static class NoOpTaskActions implements TaskActions {

		@Override
		public void triggerPartitionProducerStateCheck(
				JobID jobId,
				IntermediateDataSetID intermediateDataSetId,
				ResultPartitionID resultPartitionId) {}

		@Override
		public void failExternally(Throwable cause) {}
	}

	private static final class NoOpResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

		@Override
		public void notifyPartitionConsumable(JobID j, ResultPartitionID p, TaskActions t) {}
	}
}
