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

package org.apache.flink.table.examples.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Scanner;

public class SqlCli {

	public static void main(String[] args) throws Exception {
		final Scanner in = new Scanner(System.in);

		while (true) {
			String line = in.nextLine();

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);

			Table result = tEnv.sql(line);
			tEnv.toAppendStream(result, Row.class).print();

			env.execute();
		}
	}
}
