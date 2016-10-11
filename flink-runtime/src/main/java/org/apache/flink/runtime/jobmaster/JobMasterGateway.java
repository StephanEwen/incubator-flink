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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.PartitionState;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.message.ClassloadingProps;
import org.apache.flink.runtime.jobmaster.message.NextInputSplit;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import java.util.UUID;

/**
 * {@link JobMaster} rpc gateway interface
 */
public interface JobMasterGateway extends RpcGateway {

	// ------------------------------------------------------------------------
	//  Job start and stop methods
	// ------------------------------------------------------------------------

	void startJobExecution();

	void suspendExecution(Throwable cause);

	// ------------------------------------------------------------------------

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param leaderSessionID    The leader id of JobManager
	 * @param taskExecutionState New task execution state for a given task
	 * @return Future flag of the task execution state update result
	 */
	Future<Boolean> updateTaskExecutionState(
		final UUID leaderSessionID,
		final TaskExecutionState taskExecutionState) throws Exception;

	/**
	 * Requesting next input split for the {@link ExecutionJobVertex}. The next input split is sent back to the sender
	 * as a {@link NextInputSplit} message.
	 *
	 * @param leaderSessionID  The leader id of JobManager
	 * @param vertexID         The job vertex id
	 * @param executionAttempt The execution attempt id
	 * @return The future of the input split. If there is no further input split, will return an empty object.
	 * @throws Exception if some error occurred or information mismatch.
	 */
	Future<NextInputSplit> requestNextInputSplit(
		final UUID leaderSessionID,
		final JobVertexID vertexID,
		final ExecutionAttemptID executionAttempt) throws Exception;

	/**
	 * Requests the current state of the partition.
	 * The state of a partition is currently bound to the state of the producing execution.
	 *
	 * @param leaderSessionID The leader id of JobManager
	 * @param partitionId     The partition ID of the partition to request the state of.
	 * @param taskExecutionId The execution attempt ID of the task requesting the partition state.
	 * @param taskResultId    The input gate ID of the task requesting the partition state.
	 * @return The future of the partition state
	 */
	Future<PartitionState> requestPartitionState(
		final UUID leaderSessionID,
		final ResultPartitionID partitionId,
		final ExecutionAttemptID taskExecutionId,
		final IntermediateDataSetID taskResultId) throws Exception;

	/**
	 * Notifies the JobManager about available data for a produced partition.
	 * <p>
	 * There is a call to this method for each {@link ExecutionVertex} instance once per produced
	 * {@link ResultPartition} instance, either when first producing data (for pipelined executions)
	 * or when all data has been produced (for staged executions).
	 * <p>
	 * The JobManager then can decide when to schedule the partition consumers of the given session.
	 *
	 * @param leaderSessionID The leader id of JobManager
	 * @param partitionID     The partition which has already produced data
	 */
	void scheduleOrUpdateConsumers(
		final UUID leaderSessionID,
		final ResultPartitionID partitionID) throws Exception;

	/**
	 * Notifies the JobManager about the removal of a resource.
	 *
	 * @param resourceId The ID under which the resource is registered.
	 * @param message    Optional message with details, for logging and debugging.
	 */

	void resourceRemoved(final ResourceID resourceId, final String message);

	/**
	 * Notifies the JobManager that the checkpoint of an individual task is completed.
	 *
	 * @param leaderSessionID The leader id of JobManager
	 * @param acknowledge     The acknowledge message of the checkpoint
	 */
	void acknowledgeCheckpoint(final UUID leaderSessionID, final AcknowledgeCheckpoint acknowledge) throws Exception;

	/**
	 * Notifies the JobManager that a checkpoint request could not be heeded.
	 * This can happen if a Task is already in RUNNING state but is internally not yet ready to perform checkpoints.
	 *
	 * @param leaderSessionID The leader id of JobManager
	 * @param decline         The decline message of the checkpoint
	 */
	void declineCheckpoint(final UUID leaderSessionID, final DeclineCheckpoint decline) throws Exception;

	/**
	 * Requests a {@link KvStateLocation} for the specified {@link KvState} registration name.
	 *
	 * @param registrationName Name under which the KvState has been registered.
	 * @return Future of the requested {@link KvState} location
	 */
	Future<KvStateLocation> lookupKvStateLocation(final String registrationName) throws Exception;

	/**
	 * @param jobVertexId          JobVertexID the KvState instance belongs to.
	 * @param keyGroupRange        Key group range the KvState instance belongs to.
	 * @param registrationName     Name under which the KvState has been registered.
	 * @param kvStateId            ID of the registered KvState instance.
	 * @param kvStateServerAddress Server address where to find the KvState instance.
	 */
	void notifyKvStateRegistered(
		final JobVertexID jobVertexId,
		final KeyGroupRange keyGroupRange,
		final String registrationName,
		final KvStateID kvStateId,
		final KvStateServerAddress kvStateServerAddress);

	/**
	 * @param jobVertexId      JobVertexID the KvState instance belongs to.
	 * @param keyGroupRange    Key group index the KvState instance belongs to.
	 * @param registrationName Name under which the KvState has been registered.
	 */
	void notifyKvStateUnregistered(
		JobVertexID jobVertexId,
		KeyGroupRange keyGroupRange,
		String registrationName);

	/**
	 * Notifies the JobManager to trigger a savepoint for this job.
	 *
	 * @param leaderSessionID The leader id of JobManager
	 * @return The savepoint path
	 */
	Future<String> triggerSavepoint(final UUID leaderSessionID) throws Exception;

	/**
	 * Request the classloading props of this job.
	 */
	Future<ClassloadingProps> requestClassloadingProps() throws Exception;
}
