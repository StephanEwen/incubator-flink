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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The runner for the job manager. It deals with job level leader election and make underlying job manager
 * properly reacted.
 */
public class JobManagerRunner implements LeaderContender, OnCompletionActions, FatalErrorHandler {

	private static final Logger log = LoggerFactory.getLogger(JobManagerRunner.class);

	// ------------------------------------------------------------------------

	/** Lock to ensure that this runner can deal with leader election event and job completion notifies simultaneously */
	private final Object lock = new Object();

	/** The job graph needs to run */
	private final JobGraph jobGraph;

	/** The listener to notify once the job completes - either successfully or unsuccessfully */
	private final OnCompletionActions toNotifyOnComplete;

	/** The handler to call in case of fatal (unrecoverable) errors */ 
	private final FatalErrorHandler errorHandler;

	/** Used to check whether a job needs to be run */
	private final SubmittedJobGraphStore runningJobsRegistry;

	/** Leader election for this job */
	private final LeaderElectionService leaderElectionService;

	private final JobManagerServices jobManagerServices;

	private final JobMaster jobManager;

	/** flag marking the runner as shut down */
	private volatile boolean shutdown;

	public JobManagerRunner(
			final JobGraph jobGraph,
			final Configuration configuration,
			final RpcService rpcService,
			final HighAvailabilityServices haServices,
			final OnCompletionActions toNotifyOnComplete,
			final FatalErrorHandler errorHandler) throws Exception
	{
		this(jobGraph, configuration, rpcService, haServices,
				JobManagerServices.fromConfiguration(configuration, haServices),
				toNotifyOnComplete, errorHandler);
	}

	/**
	 * 
	 * Exceptions that occur while creating the JobManager or JobManagerRunner are directly
	 * thrown and not reported to the given {@code FatalErrorHandler}.
	 * 
	 * @param jobGraph
	 * @param configuration
	 * @param rpcService
	 * @param haServices
	 * @param jobManagerServices
	 * @param toNotifyOnComplete
	 * @param errorHandler
	 * 
	 * @throws Exception
	 */
	public JobManagerRunner(
			final JobGraph jobGraph,
			final Configuration configuration,
			final RpcService rpcService,
			final HighAvailabilityServices haServices,
			final JobManagerServices jobManagerServices,
			final OnCompletionActions toNotifyOnComplete,
			final FatalErrorHandler errorHandler) throws Exception
	{
		this.jobGraph = checkNotNull(jobGraph);
		this.toNotifyOnComplete = checkNotNull(toNotifyOnComplete);
		this.errorHandler = checkNotNull(errorHandler);
		this.jobManagerServices = checkNotNull(jobManagerServices);

		checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

		// make sure we cleanly shut down out JobManager services if initialization fails

		try {
			// libraries and class loader first
			final BlobLibraryCacheManager libraryCacheManager = jobManagerServices.libraryCacheManager;
			try {
				libraryCacheManager.registerJob(
						jobGraph.getJobID(), jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths());
			} catch (IOException e) {
				throw new Exception("Cannot set up the user code libraries: " + e.getMessage(), e);
			}

			final ClassLoader userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID());
			if (userCodeLoader == null) {
				throw new Exception("The user code class loader could not be initialized.");
			}

			// high availability services next
			this.runningJobsRegistry = haServices.getSubmittedJobGraphStore();
			this.leaderElectionService = haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

			this.jobManager = new JobMaster(
					jobGraph, configuration, rpcService, haServices,
					jobManagerServices.libraryCacheManager,
					jobManagerServices.restartStrategyFactory,
					jobManagerServices.savepointStore,
					jobManagerServices.timeout,
					new Scheduler(jobManagerServices.executorService),
					jobManagerServices.jobManagerMetricGroup,
					this,
					this,
					userCodeLoader);
			
		}
		catch (Throwable t) {
			// clean up everything
			try {
				jobManagerServices.shutdown();
			} catch (Throwable tt) {
				log.error("Error while shutting down JobManager services", tt);
			}

			throw new JobExecutionException(jobGraph.getJobID(), "Could not set up JobManager", t);
		}
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	public void start() throws Exception {
		jobManager.start();

		try {
			leaderElectionService.start(this);
		}
		catch (Exception e) {
			log.error("Could not start the JobManager because the leader election service did not start.", e);
			throw new Exception("Could not start the leader election service.", e);
		}
	}

	public void shutdown() {
		shutdown(new Exception("The JobManager runner is shutting down"));
	}

	public void shutdown(Throwable cause) {
		// TODO what is the cause used for ?
		shutdownInternally();
	}

	private void shutdownInternally() {
		synchronized (lock) {
			shutdown = true;

			if (leaderElectionService != null) {
				try {
					leaderElectionService.stop();
				} catch (Exception e) {
					log.error("Could not properly shutdown the leader election service.");
				}
			}

			jobManager.shutDown();
		}
	}

	//----------------------------------------------------------------------------------------------
	// Result and error handling methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFinished(JobExecutionResult result) {
		try {
			unregisterJobFromHighAvailability();
			shutdownInternally();
		}
		finally {
			if (toNotifyOnComplete != null) {
				toNotifyOnComplete.jobFinished(result);
			}
		}
	}

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFailed(Throwable cause) {
		try {
			unregisterJobFromHighAvailability();
			shutdownInternally();
		}
		finally {
			if (toNotifyOnComplete != null) {
				toNotifyOnComplete.jobFailed(cause);
			}
		}
	}

	/**
	 * Job completion notification triggered by self
	 */
	@Override
	public void jobFinishedByOther() {
		try {
			unregisterJobFromHighAvailability();
			shutdownInternally();
		}
		finally {
			if (toNotifyOnComplete != null) {
				toNotifyOnComplete.jobFinishedByOther();
			}
		}
	}

	/**
	 * Job completion notification triggered by JobManager or self
	 */
	@Override
	public void onFatalError(Throwable exception) {
		// we log first to make sure an explaining message goes into the log
		// we even guard the log statement here to increase chances that the error handler
		// gets the notification on hard critical situations like out-of-memory errors
		try {
			log.error("JobManager runner encountered a fatal error.", exception);
		} catch (Throwable ignored) {}

		// in any case, notify our handler, so it can react fast
		try {
			if (errorHandler != null) {
				errorHandler.onFatalError(exception);
			}
		}
		finally {
			// the shutdown may not even needed any more, if the fatal error
			// handler kills the process. that is fine, a process kill cleans up better than anything.
			shutdownInternally();
		}
	}

	/**
	 * Marks this runner's job as not running. Other JobManager will not recover the job
	 * after this call.
	 * 
	 * <p>This method never throws an exception.
	 */
	private void unregisterJobFromHighAvailability() {
		try {
			runningJobsRegistry.removeJobGraph(jobGraph.getJobID());
		}
		catch (Throwable t) {
			log.error("Could not un-register from high-availability services job {} ({})." +
					"Other JobManager's may attempt to recover it and re-execute it.",
					jobGraph.getName(), jobGraph.getJobID(), t);
		}
	}

	//----------------------------------------------------------------------------------------------
	// Leadership methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void grantLeadership(final UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				log.info("JobManagerRunner already shutdown.");
				return;
			}

			log.info("JobManager runner for job {} ({}) was granted leadership with session id {} at {}.",
				jobGraph.getName(), jobGraph.getJobID(), leaderSessionID, getAddress());

			// The operation may be blocking, but since this runner is idle before it been granted the leadership,
			// it's okay that job manager wait for the operation complete
			leaderElectionService.confirmLeaderSessionID(leaderSessionID);

			// Double check the leadership after we confirm that, there is a small chance that multiple
			// job managers schedule the same job after if they try to recover at the same time.
			// This will eventually be noticed, but can not be ruled out from the beginning.
			if (leaderElectionService.hasLeadership()) {
				if (isJobFinishedByOthers()) {
					log.info("Job {} ({}) already finished by others.", jobGraph.getName(), jobGraph.getJobID());
					jobFinishedByOther();
				} else {
					jobManager.start(leaderSessionID);
				}
			}
		}
	}

	@Override
	public void revokeLeadership() {
		synchronized (lock) {
			if (shutdown) {
				log.info("JobManagerRunner already shutdown.");
				return;
			}

			log.info("JobManager for job {} ({}) was revoked leadership at {}.",
				jobGraph.getName(), jobGraph.getJobID(), getAddress());

			jobManager.suspend(new Exception("JobManager is no longer the leader."));
		}
	}

	@Override
	public String getAddress() {
		return jobManager.getAddress();
	}

	@Override
	public void handleError(Exception exception) {
		log.error("Leader Election Service encountered a fatal error.", exception);
		onFatalError(exception);
	}

	@VisibleForTesting
	boolean isJobFinishedByOthers() {
		// TODO: Fix
		return false;
	}

	@VisibleForTesting
	boolean isShutdown() {
		return shutdown;
	}
}
