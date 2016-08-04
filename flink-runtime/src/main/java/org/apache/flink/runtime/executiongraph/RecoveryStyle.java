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

package org.apache.flink.runtime.executiongraph;

/**
 * The {@code RecoveryStyle} defines which mechanism Flink uses to recover from failures.
 * Different options have different restart granularity, meaning the restart more or fewer tasks
 * upon failures.
 * 
 * <p>The strategies apply in the same way to streaming (infinite) and batch (finite) programs,
 * or to programs with- and without checkpointing:
 * <ul>
 *     <li>If checkpointing is <b>activated</b>, then all task restarts will restart the task from
 *         the <b>latest completed checkpoint</b>.</li>
 *     <li>If checkpointing is <b>deactivated</b>, then all task restarts will restart the task blank.
 *         For streaming programs, tasks are started with empty state. For batch programs, tasks
 *         are restarted fully.</li> 
 * </ul> 
 */
public enum RecoveryStyle {

	/** This style indicates that upon a failure, the entire program is restarted from the latest checkpoint.
	 * For batch programs that do not use checkpoints, the entire program will be restarted. */
	FULL_RESTART,

	/** Upon a failure, the failed task is restarted (from the latest checkpoint). Predecessors to the task
	 * are recursively restarted as needed. A predecessor task needs to be restarted, if their produced
	 * data (since the last checkpoint) is not cached any more and the restarted task could otherwise not
	 * resume its input.
	 * 
	 * <p>Whether a predecessor task's produced data is still cached depends on its configuration. 
	 * 
	 * <p>Tasks that consume data from any restarted task (i.e., downstream tasks)
	 * are canceled and restarted as well. That way, this strategy does not lead to inconsistencies
	 * in the presence of non-deterministic tasks. */
	BACKTRACKING,
}
