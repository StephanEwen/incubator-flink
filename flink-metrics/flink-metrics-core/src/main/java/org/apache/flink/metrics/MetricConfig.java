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
package org.apache.flink.metrics;

import java.util.Properties;

public class MetricConfig extends Properties {

	// ------------------------------------------------------------------------
	//  Configuration keys
	// ------------------------------------------------------------------------

	/** The class of the reporter to use. */
	public static final String METRICS_REPORTER_CLASS = "metrics.reporter.class";

	/** A list of named parameters that are passed to the reporter. */
	public static final String METRICS_REPORTER_ARGUMENTS = "metrics.reporter.arguments";

	/** The interval between reports. */
	public static final String METRICS_REPORTER_INTERVAL = "metrics.reporter.interval";

	/** The delimiter used to assemble the metric identifier. */
	public static final String METRICS_SCOPE_DELIMITER = "metrics.scope.delimiter";

	/** The scope format string that is applied to all metrics scoped to a JobManager. */
	public static final String METRICS_SCOPE_NAMING_JM = "metrics.scope.jm";

	/** The scope format string that is applied to all metrics scoped to a TaskManager. */
	public static final String METRICS_SCOPE_NAMING_TM = "metrics.scope.tm";

	/** The scope format string that is applied to all metrics scoped to a job on a JobManager. */
	public static final String METRICS_SCOPE_NAMING_JM_JOB = "metrics.scope.jm.job";

	/** The scope format string that is applied to all metrics scoped to a job on a TaskManager. */
	public static final String METRICS_SCOPE_NAMING_TM_JOB = "metrics.scope.tm.job";

	/** The scope format string that is applied to all metrics scoped to a task. */
	public static final String METRICS_SCOPE_NAMING_TASK = "metrics.scope.task";

	/** The scope format string that is applied to all metrics scoped to an operator. */
	public static final String METRICS_SCOPE_NAMING_OPERATOR = "metrics.scope.operator";

	// ------------------------------------------------------------------------
	//  Configuration keys
	// ------------------------------------------------------------------------

	public String getString(String key, String defaultValue) {
		return getProperty(key, defaultValue);
	}

	public int getInteger(String key, int defaultValue) {
		String argument = getProperty(key, null);
		return argument == null
			? defaultValue
			: Integer.parseInt(argument);
	}

	public long getLong(String key, long defaultValue) {
		String argument = getProperty(key, null);
		return argument == null
			? defaultValue
			: Long.parseLong(argument);
	}

	public float getFloat(String key, float defaultValue) {
		String argument = getProperty(key, null);
		return argument == null
			? defaultValue
			: Float.parseFloat(argument);
	}

	public double getDouble(String key, double defaultValue) {
		String argument = getProperty(key, null);
		return argument == null
			? defaultValue
			: Double.parseDouble(argument);
	}

	public boolean getBoolean(String key, boolean defaultValue) {
		String argument = getProperty(key, null);
		return argument == null
			? defaultValue
			: Boolean.parseBoolean(argument);
	}
}
