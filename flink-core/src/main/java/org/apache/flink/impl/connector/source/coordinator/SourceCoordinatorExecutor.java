/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.impl.connector.source.coordinator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SourceCoordinatorExecutor {
	private final BlockingQueue<Runnable> taskQueue;
	private final ScheduledExecutorService scheduledExecutor;

	SourceCoordinatorExecutor() {
		this.taskQueue = new LinkedBlockingQueue<>();
		this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
				r -> new Thread("SourceCoordinatorScheduledExecutor"));
	}

	public void execute(Runnable command) {
		taskQueue.add(command);
	}

	public void execute(Runnable command, long delayMs) {
		scheduledExecutor.schedule(() -> taskQueue.add(command), delayMs, TimeUnit.MILLISECONDS);
	}

	public void scheduleAtFixedRate(Runnable command, long intervalMs) {
		scheduleAtFixedRate(command, 0, intervalMs);
	}

	public void scheduleAtFixedRate(Runnable command, long initialDelayMs, long intervalMs) {
		scheduledExecutor.scheduleAtFixedRate(() -> taskQueue.add(command), initialDelayMs, intervalMs, TimeUnit.MILLISECONDS);
	}
}
