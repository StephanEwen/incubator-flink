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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.CapacityAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.IOMetricGroup;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup.DummyIOMetricGroup;

import org.junit.Test;

import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.mock;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createMockBuffer;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createResultPartitionManager;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createDummyConnectionManager;

public class InputGateFairnessTest {

	@Test
	public void testFairConsumptionLocalChannelsPreFilled() throws Exception {
		final int numChannels = 37;
		final int buffersPerChannel = 27;

		final ResultPartition resultPartition = mock(ResultPartition.class);
		final Buffer mockBuffer = createMockBuffer(42);

		// ----- create some source channels and fill them with buffers -----

		final PipelinedSubpartition[] sources = new PipelinedSubpartition[numChannels];

		for (int i = 0; i < numChannels; i++) {
			PipelinedSubpartition partition = new PipelinedSubpartition(0, resultPartition, 0);

			for (int p = 0; p < buffersPerChannel; p++) {
				partition.add(mockBuffer, false);
			}

			partition.finish();
			sources[i] = partition;
		}

		// ----- create reading side -----

		ResultPartitionManager resultPartitionManager = createResultPartitionManager(sources);

		SingleInputGate gate = new FairnessVerifyingInputGate(
				"Test Task Name",
				new JobID(),
				new ExecutionAttemptID(),
				new IntermediateDataSetID(),
				0, numChannels,
				mock(PartitionStateChecker.class),
				new UnregisteredTaskMetricsGroup.DummyIOMetricGroup());

		for (int i = 0; i < numChannels; i++) {
			LocalInputChannel channel = new LocalInputChannel(gate, i, new ResultPartitionID(),
					resultPartitionManager, mock(TaskEventDispatcher.class), new DummyIOMetricGroup());
			gate.setInputChannel(new IntermediateResultPartitionID(), channel);
		}

		// read all the buffers and the EOF event
		for (int i = numChannels * (buffersPerChannel + 1); i > 0; --i) {
			assertNotNull(gate.getNextBufferOrEvent());

			int min = Integer.MAX_VALUE;
			int max = 0;

			for (PipelinedSubpartition source : sources) {
				int size = source.getCurrentNumberOfBuffers();
				min = Math.min(min, size);
				max = Math.max(max, size);
			}

			assertTrue(max == min || max == min+1);
		}

		assertNull(gate.getNextBufferOrEvent());
	}

	@Test
	public void testFairConsumptionLocalChannels() throws Exception {
		final int numChannels = 37;
		final int buffersPerChannel = 27;

		final ResultPartition resultPartition = mock(ResultPartition.class);
		final Buffer mockBuffer = createMockBuffer(42);

		// ----- create some source channels and fill them with one buffer each -----

		final PipelinedSubpartition[] sources = new PipelinedSubpartition[numChannels];

		for (int i = 0; i < numChannels; i++) {
			sources[i] = new PipelinedSubpartition(0, resultPartition, 0);
		}

		// ----- create reading side -----

		ResultPartitionManager resultPartitionManager = createResultPartitionManager(sources);

		SingleInputGate gate = new FairnessVerifyingInputGate(
				"Test Task Name",
				new JobID(),
				new ExecutionAttemptID(),
				new IntermediateDataSetID(),
				0, numChannels,
				mock(PartitionStateChecker.class),
				new UnregisteredTaskMetricsGroup.DummyIOMetricGroup());

		for (int i = 0; i < numChannels; i++) {
			LocalInputChannel channel = new LocalInputChannel(gate, i, new ResultPartitionID(),
					resultPartitionManager, mock(TaskEventDispatcher.class), new DummyIOMetricGroup());
			gate.setInputChannel(new IntermediateResultPartitionID(), channel);
		}

		// seed one initial buffer
		sources[12].add(mockBuffer, false);

		// read all the buffers and the EOF event
		for (int i = 0; i < numChannels * buffersPerChannel; i++) {
			assertNotNull(gate.getNextBufferOrEvent());

			int min = Integer.MAX_VALUE;
			int max = 0;

			for (PipelinedSubpartition source : sources) {
				int size = source.getCurrentNumberOfBuffers();
				min = Math.min(min, size);
				max = Math.max(max, size);
			}

			assertTrue(max == min || max == min+1);

			if (i % (2 * numChannels) == 0) {
				// add three buffers to each channel, in random order
				fillRandom(sources, 3, mockBuffer);
			}
		}

		// there is still more in the queues
	}

	@Test
	public void testFairConsumptionRemoteChannelsPreFilled() throws Exception {
		final int numChannels = 37;
		final int buffersPerChannel = 27;

		final Buffer mockBuffer = createMockBuffer(42);

		// ----- create some source channels and fill them with buffers -----

		SingleInputGate gate = new FairnessVerifyingInputGate(
				"Test Task Name",
				new JobID(),
				new ExecutionAttemptID(),
				new IntermediateDataSetID(),
				0, numChannels,
				mock(PartitionStateChecker.class),
				new UnregisteredTaskMetricsGroup.DummyIOMetricGroup());

		final ConnectionManager connManager = createDummyConnectionManager();
		final CapacityAvailabilityListener capacityListener = mock(CapacityAvailabilityListener.class);

		final RemoteInputChannel[] channels = new RemoteInputChannel[numChannels];

		for (int i = 0; i < numChannels; i++) {
			RemoteInputChannel channel = new RemoteInputChannel(
					gate, i, new ResultPartitionID(), mock(ConnectionID.class), 
					connManager, new Tuple2<>(0, 0), new DummyIOMetricGroup(), 0);

			channels[i] = channel;
			
			for (int p = 0; p < buffersPerChannel; p++) {
				channel.onBuffer(mockBuffer, p, capacityListener);
			}
			channel.onBuffer(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE), buffersPerChannel, capacityListener);

			gate.setInputChannel(new IntermediateResultPartitionID(), channel);
		}

		// read all the buffers and the EOF event
		for (int i = numChannels * (buffersPerChannel + 1); i > 0; --i) {
			assertNotNull(gate.getNextBufferOrEvent());

			int min = Integer.MAX_VALUE;
			int max = 0;

			for (RemoteInputChannel channel : channels) {
				int size = channel.getNumberOfQueuedBuffers();
				min = Math.min(min, size);
				max = Math.max(max, size);
			}

			assertTrue(max == min || max == min+1);
		}

		assertNull(gate.getNextBufferOrEvent());
	}

	@Test
	public void testFairConsumptionRemoteChannels() throws Exception {
		final int numChannels = 37;
		final int buffersPerChannel = 27;

		final Buffer mockBuffer = createMockBuffer(42);

		// ----- create some source channels and fill them with buffers -----

		SingleInputGate gate = new FairnessVerifyingInputGate(
				"Test Task Name",
				new JobID(),
				new ExecutionAttemptID(),
				new IntermediateDataSetID(),
				0, numChannels,
				mock(PartitionStateChecker.class),
				new UnregisteredTaskMetricsGroup.DummyIOMetricGroup());

		final ConnectionManager connManager = createDummyConnectionManager();
		final CapacityAvailabilityListener capacityListener = mock(CapacityAvailabilityListener.class);

		final RemoteInputChannel[] channels = new RemoteInputChannel[numChannels];
		final int[] channelSequenceNums = new int[numChannels];

		for (int i = 0; i < numChannels; i++) {
			RemoteInputChannel channel = new RemoteInputChannel(
					gate, i, new ResultPartitionID(), mock(ConnectionID.class),
					connManager, new Tuple2<>(0, 0), new DummyIOMetricGroup(), 0);

			channels[i] = channel;
			gate.setInputChannel(new IntermediateResultPartitionID(), channel);
		}

		channels[11].onBuffer(mockBuffer, 0, capacityListener);
		channelSequenceNums[11]++;

		// read all the buffers and the EOF event
		for (int i = 0; i < numChannels * buffersPerChannel; i++) {
			assertNotNull(gate.getNextBufferOrEvent());

			int min = Integer.MAX_VALUE;
			int max = 0;

			for (RemoteInputChannel channel : channels) {
				int size = channel.getNumberOfQueuedBuffers();
				min = Math.min(min, size);
				max = Math.max(max, size);
			}

			assertTrue(max == min || max == min+1);

			if (i % (2 * numChannels) == 0) {
				// add three buffers to each channel, in random order
				fillRandom(channels, channelSequenceNums, 3, mockBuffer);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void fillRandom(PipelinedSubpartition[] partitions, int numPerPartition, Buffer buffer) throws Exception {
		ArrayList<Integer> poss = new ArrayList<>(partitions.length * numPerPartition);

		for (int i = 0; i < partitions.length; i++) {
			for (int k = 0; k < numPerPartition; k++) {
				poss.add(i);
			}
		}

		Collections.shuffle(poss);

		for (Integer i : poss) {
			partitions[i].add(buffer, false);
		}
	}

	private void fillRandom(
			RemoteInputChannel[] partitions,
			int[] sequenceNumbers,
			int numPerPartition,
			Buffer buffer) throws Exception {

		ArrayList<Integer> poss = new ArrayList<>(partitions.length * numPerPartition);

		for (int i = 0; i < partitions.length; i++) {
			for (int k = 0; k < numPerPartition; k++) {
				poss.add(i);
			}
		}

		Collections.shuffle(poss);
		CapacityAvailabilityListener listener = mock(CapacityAvailabilityListener.class);

		for (int i : poss) {
			partitions[i].onBuffer(buffer, sequenceNumbers[i]++, listener);
		}
	}
	
	// ------------------------------------------------------------------------

	private static class FairnessVerifyingInputGate extends SingleInputGate {

		private final ArrayDeque<InputChannel> channelsWithData;

		private final HashSet<InputChannel> uniquenessChecker;

		@SuppressWarnings("unchecked")
		public FairnessVerifyingInputGate(
				String owningTaskName,
				JobID jobId,
				ExecutionAttemptID executionId,
				IntermediateDataSetID consumedResultId,
				int consumedSubpartitionIndex,
				int numberOfInputChannels,
				PartitionStateChecker partitionStateChecker,
				IOMetricGroup metrics) {

			super(owningTaskName, jobId, executionId, consumedResultId, consumedSubpartitionIndex,
					numberOfInputChannels, partitionStateChecker, metrics);

			try {
				Field f = SingleInputGate.class.getDeclaredField("inputChannelsWithData");
				f.setAccessible(true);
				channelsWithData = (ArrayDeque<InputChannel>) f.get(this);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}

			this.uniquenessChecker = new HashSet<>();
		}


		@Override
		public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {
			synchronized (channelsWithData) {
				assertTrue("too many input channels", channelsWithData.size() <= getNumberOfInputChannels());
				ensureUnique(channelsWithData);
			}

			return super.getNextBufferOrEvent();
		}

		private void ensureUnique(Collection<InputChannel> channels) {
			HashSet<InputChannel> uniquenessChecker = this.uniquenessChecker;

			for (InputChannel channel : channels) {
				if (!uniquenessChecker.add(channel)) {
					fail("Duplicate channel in input gate: " + channel);
				}
			}

			assertTrue("found duplicate input channels", uniquenessChecker.size() == channels.size());
			uniquenessChecker.clear();
		}
	}
}
