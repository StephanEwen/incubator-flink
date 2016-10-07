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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStore;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.util.ExceptionUtils;


import java.util.concurrent.ExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to hold all auxiliary services used by the {@link JobMaster}.
 */
public class JobManagerServices {

	public final ExecutorService executorService;

	public final BlobLibraryCacheManager libraryCacheManager;

	public final RestartStrategyFactory restartStrategyFactory;

	public final SavepointStore savepointStore;

	public final Time timeout;

	public final JobManagerMetricGroup jobManagerMetricGroup;

	public final JobManagerJobMetricGroup jobManagerJobMetricGroup;

	public JobManagerServices(
			ExecutorService executorService,
			BlobLibraryCacheManager libraryCacheManager,
			RestartStrategyFactory restartStrategyFactory,
			SavepointStore savepointStore,
			Time timeout,
			JobManagerMetricGroup jobManagerMetricGroup,
			JobManagerJobMetricGroup jobManagerJobMetricGroup) {

		this.executorService = checkNotNull(executorService);
		this.libraryCacheManager = checkNotNull(libraryCacheManager);
		this.restartStrategyFactory = checkNotNull(restartStrategyFactory);
		this.savepointStore = checkNotNull(savepointStore);
		this.timeout = checkNotNull(timeout);
		this.jobManagerMetricGroup = checkNotNull(jobManagerMetricGroup);
		this.jobManagerJobMetricGroup = checkNotNull(jobManagerJobMetricGroup);
	}

	/**
	 * 
	 * <p>This method makes sure all services are closed or shut down, even when an exception occurred
	 * in the shutdown of one component. The first encountered exception is thrown, with successive
	 * exceptions added as suppressed exceptions.
	 * 
	 * @throws Exception The first Exception encountered during shutdown.
	 */
	public void shutdown() throws Exception {
		Throwable firstException = null;

		try {
			executorService.shutdownNow();
		} catch (Throwable t) {
			firstException = t;
		}

		try {
			jobManagerJobMetricGroup.close();
			jobManagerMetricGroup.close();
		}
		catch (Throwable t) {
			if (firstException == null) {
				firstException = t;
			} else {
				firstException.addSuppressed(t);
			}
		}

		try {
			savepointStore.shutdown();
		}
		catch (Throwable t) {
			if (firstException == null) {
				firstException = t;
			} else {
				firstException.addSuppressed(t);
			}
		}

		try {
			libraryCacheManager.shutdown();
		}
		catch (Throwable t) {
			if (firstException == null) {
				firstException = t;
			} else {
				firstException.addSuppressed(t);
			}
		}

		if (firstException != null) {
			ExceptionUtils.rethrowException(firstException, "Error while shutting down JobManager services");
		}
	}

	// ------------------------------------------------------------------------
	//  Creating the components from a configuration 
	// ------------------------------------------------------------------------
	
	public static JobManagerServices fromConfiguration(
			Configuration config,
			HighAvailabilityServices haServices) throws Exception {

		// TODO - not implemented, yet
		return null;
	}
}
