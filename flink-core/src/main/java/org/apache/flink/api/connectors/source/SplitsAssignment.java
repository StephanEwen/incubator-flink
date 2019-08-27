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

package org.apache.flink.api.connectors.source;

import java.util.List;
import java.util.Map;

/**
 * A class containing the splits assignment to the source readers.
 *
 * <p>The assignment may have two types: incremental or overriding. The incremental assignment
 * simply adds the splits to the existing assignment. The overriding assignment will replace
 * the existing assignment.
 */
public class SplitsAssignment<SplitT extends SourceSplit> {
	private final Type type;
	private final Map<Integer, List<SplitT>> assignment;

	public SplitsAssignment(Map<Integer, List<SplitT>> assignment) {
		this(assignment, Type.INCREMENTAL);
	}

	public SplitsAssignment(Map<Integer, List<SplitT>> assignment, Type type) {
		this.assignment = assignment;
		this.type = type;
	}

	public Map<Integer, List<SplitT>> assignment() {
		return assignment;
	}

	public Type type() {
		return type;
	}

	public enum Type {
		INCREMENTAL,
		OVERRIDING
	}
}
