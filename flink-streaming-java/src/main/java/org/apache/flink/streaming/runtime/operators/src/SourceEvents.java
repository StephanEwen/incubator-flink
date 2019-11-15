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

package org.apache.flink.streaming.runtime.operators.src;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The set of events in the protocol between Source Reader and Source Coordinator.
 */
public final class SourceEvents {

	/**
	 * An event sent from the {@link SourceReaderOperator} to the {@link SourceCoordinator} to request one source split.
	 */
	public static final class RequestSplitEvent extends SourceEvent {
		private static final long serialVersionUID = 1L;

		@Override
		public String toString() {
			return "request input split";
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * An event sent from the {@link SourceCoordinator} to the {@link SourceReaderOperator} to assign
	 * an input split.
	 */
	public static final class AssignSplitEvent extends SourceEvent {
		private static final long serialVersionUID = 1L;

		private final String sourceSplit; // TODO make this the real source split

		public AssignSplitEvent(String sourceSplit) {
			this.sourceSplit = checkNotNull(sourceSplit);
		}

		public String sourceSplit() {
			return sourceSplit;
		}

		@Override
		public String toString() {
			return "assign input split: " + sourceSplit;
		}
	}

	// ------------------------------------------------------------------------

	/** This utility / container class is not meant to be instantiated. */
	private SourceEvents() {}
}
