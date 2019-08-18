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

package org.apache.flink.testutils;

import java.util.function.Supplier;

/**
 * A utils class that contains methods that are useful for unit tests in general.
 */
public class TestUtils {
	// Private constructor for util classes.
	private TestUtils() {}

	/**
	 * Wait until the given condition becomes true.
	 *
	 * @param condition the condition to wait.
	 * @throws InterruptedException if the waiting is interrupted.
	 */
	public static void waitUntil(Supplier<Boolean> condition) throws InterruptedException {
		while (!condition.get()) {
			Thread.sleep(1);
		}
	}
}
