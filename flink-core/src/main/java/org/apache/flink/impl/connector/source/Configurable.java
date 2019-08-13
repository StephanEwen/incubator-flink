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

package org.apache.flink.impl.connector.source;

import org.apache.flink.configuration.Configuration;

/**
 * An interface allowing a instantiate-then-configure pattern of pluggables. This class
 * should be in a more generic package and applied to all the pluggables in Flink. For now
 * we just keep it in the connector common for development purpose.
 */
public interface Configurable {

	/**
	 * Configure the implementation class with the given configurations.
	 * @param config the configuration to use.
	 */
	void configure(Configuration config);
}
