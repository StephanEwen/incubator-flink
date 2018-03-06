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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.librarycache.NoLibrariesCacheManager;
import org.apache.flink.runtime.executiongraph.DummyJobInformation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateManager;

import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utilities to instantiate the services needed to test Tasks.
 */
public class TaskTestUtils {

	public static Task createTask(String invokableClassName) throws IOException {
		return createTask(invokableClassName, new Configuration());
	}

	public static Task createTask(
			String invokableClassName,
			Configuration taskManagerConfig) throws IOException {

		return new Task(
				new DummyJobInformation(),
				new TaskInformation(
						new JobVertexID(),
						"test task",
						1,
						1,
						invokableClassName,
						new Configuration()),
				new ExecutionAttemptID(),
				new AllocationID(),
				0,
				0,
				Collections.emptyList(),
				Collections.emptyList(),
				0,
				Mockito.mock(MemoryManager.class),
				Mockito.mock(IOManager.class),
				TaskTestUtils.createTestNetworkEnvironment(),
				Mockito.mock(BroadcastVariableManager.class),
				Mockito.mock(TaskStateManager.class),
				Mockito.mock(TaskManagerActions.class),
				Mockito.mock(InputSplitProvider.class),
				Mockito.mock(CheckpointResponder.class),
				new BlobCacheService(Mockito.mock(PermanentBlobCache.class), Mockito.mock(TransientBlobCache.class)),
				new NoLibrariesCacheManager(),
				new FileCache(new String[]{CommonTestUtils.getTempDir()}),
				new TaskManagerRuntimeInfo() {
					@Override
					public Configuration getConfiguration() {
						return taskManagerConfig;
					}

					@Override
					public String[] getTmpDirectories() {
						return new String[]{CommonTestUtils.getTempDir()};
					}

					@Override
					public boolean shouldExitJvmOnOutOfMemoryError() {
						return false;
					}
				},
				UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
				Mockito.mock(ResultPartitionConsumableNotifier.class),
				Mockito.mock(PartitionProducerStateChecker.class),
				Executors.directExecutor());
	}

	public static NetworkEnvironment createTestNetworkEnvironment() {
		TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);

		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));
		when(network.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);

		return network;
	}
}
