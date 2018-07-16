package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer;

import java.io.IOException;

/**
 * An interface exposing the information concerning the current (open) part file
 * that is necessary to the {@link RollingPolicy} in order to determine if it
 * should roll the part file or not.
 */
@PublicEvolving
public interface PartFileInfo {

	/**
	 * @return The bucket identifier of the current buffer, as returned by the
	 * {@link Bucketer#getBucketId(Object, Bucketer.Context)}.
	 */
	String getBucketId();

	/**
	 * @return The creation time (in ms) of the currently open part file.
	 */
	long getCreationTime();

	/**
	 * @return The size of the currently open part file.
	 */
	long getSize() throws IOException;

	/**
	 * @return The last time (in ms) the currently open part file was written to.
	 */
	long getLastUpdateTime();
}
