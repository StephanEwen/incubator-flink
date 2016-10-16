package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotPool;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple pool based slot provider with {@link SlotPool} as the underlying storage.
 */
public class PooledSlotProvider implements SlotProvider {

	/** The pool which holds all the slots. */
	private final SlotPool slotPool;

	/** The timeout for allocation. */
	private final Time timeout;

	public PooledSlotProvider(final SlotPool slotPool, final Time timeout) {
		this.slotPool = slotPool;
		this.timeout = timeout;
	}

	@Override
	public Future<SimpleSlot> allocateSlot(ScheduledUnit task,
			boolean allowQueued) throws NoResourceAvailableException
	{
		checkNotNull(task);

		final JobID jobID = task.getTaskToExecute().getVertex().getJobId();
		final Future<SimpleSlot> future = slotPool.allocateSimpleSlot(jobID, ResourceProfile.UNKNOWN);
		try {
			final SimpleSlot slot = future.get(timeout.getSize(), timeout.getUnit());
			return FlinkCompletableFuture.completed(slot);
		} catch (InterruptedException e) {
			throw new NoResourceAvailableException("Could not allocate a slot because it's interrupted.");
		} catch (ExecutionException e) {
			throw new NoResourceAvailableException("Could not allocate a slot because some error occurred " +
					"during allocation, " + e.getMessage());
		} catch (TimeoutException e) {
			throw new NoResourceAvailableException("Could not allocate a slot within time limit: " + timeout);
		}
	}
}
