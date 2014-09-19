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

package org.apache.flink.runtime.memorymanager;

/**
 * Category for a memory allocation. The category defines the priority that the requests are given with
 * respect to other allocation requests.
 */
public enum AllocationCategory {

	/** Allocation category for allocations that, if not served, fail a program */
	CRITICAL(0),
	
	/** Allocation category for runtime operations (sorting, hashing) */
	OPERATOR(1),
	
	/** Allocation category for intermediate results that are being produced or have not been consumed by
	 * all consumers yet */
	INTERMEDIATE_RESULT(2),
	
	/** Category for intermediate results that have been consumed and are kept on a FIFO basis. */
	HISTORIC_INTERMEDIATE_RESULT(3);
	
	// --------------------------------------------------------------------------------------------
	
	private final int priority;
	
	private AllocationCategory(int priority) {
		this.priority = priority;
	}
	
	public int getPriority() {
		return priority;
	}
	
	public boolean samePriority(AllocationCategory other) {
		if (other == null) {
			throw new IllegalArgumentException("null");
		}
		
		return this.priority == other.priority;
	}
	
	public boolean hasPriorityOver(AllocationCategory other) {
		if (other == null) {
			throw new IllegalArgumentException("null");
		}
		
		return this.priority > other.priority;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static int getNumberOfCategories() {
		return values().length;
	}
}
