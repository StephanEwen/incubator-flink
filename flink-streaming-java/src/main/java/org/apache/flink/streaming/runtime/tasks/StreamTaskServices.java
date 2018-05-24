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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.runtime.tasks.StreamTask.AsyncCheckpointExceptionHandler;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutorService;

/**
 * This class is used to pass test implementations of certain components to the StreamTask.
 */
@VisibleForTesting
class StreamTaskServices {

	@Nullable
	private ProcessingTimeService processingTimeService;

	@Nullable
	private CheckpointExceptionHandlerFactory synchronousCheckpointExceptionHandler;

	@Nullable
	private AsyncCheckpointExceptionHandler asynchronousCheckpointExceptionHandler;

	@Nullable
	private ExecutorService asyncOperationsThreadPool;

	// ------------------------------------------------------------------------
	// setters
	// ------------------------------------------------------------------------

	public StreamTaskServices setProcessingTimeService(ProcessingTimeService processingTimeService) {
		this.processingTimeService = processingTimeService;
		return this;
	}

	public StreamTaskServices setSynchronousCheckpointExceptionHandler(CheckpointExceptionHandlerFactory synchronousCheckpointExceptionHandler) {
		this.synchronousCheckpointExceptionHandler = synchronousCheckpointExceptionHandler;
		return this;
	}

	public StreamTaskServices setAsynchronousCheckpointExceptionHandler(AsyncCheckpointExceptionHandler asynchronousCheckpointExceptionHandler) {
		this.asynchronousCheckpointExceptionHandler = asynchronousCheckpointExceptionHandler;
		return this;
	}

	public StreamTaskServices setAsyncOperationsThreadPool(ExecutorService asyncOperationsThreadPool) {
		this.asyncOperationsThreadPool = asyncOperationsThreadPool;
		return this;
	}

	// ------------------------------------------------------------------------
	//  getters
	// ------------------------------------------------------------------------

	@Nullable
	public ProcessingTimeService getProcessingTimeService() {
		return processingTimeService;
	}

	@Nullable
	public ExecutorService getAsyncOperationsThreadPool() {
		return asyncOperationsThreadPool;
	}

	@Nullable
	public CheckpointExceptionHandlerFactory getSynchronousCheckpointExceptionHandler() {
		return synchronousCheckpointExceptionHandler;
	}

	@Nullable
	public AsyncCheckpointExceptionHandler getAsynchronousCheckpointExceptionHandler() {
		return asynchronousCheckpointExceptionHandler;
	}
}
