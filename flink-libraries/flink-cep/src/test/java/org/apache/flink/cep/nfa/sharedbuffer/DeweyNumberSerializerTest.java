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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.cep.nfa.DeweyNumber.DeweyNumberSerializer;

/**
 * A test for the DeweyNumberSerializer.
 */
public class DeweyNumberSerializerTest extends SerializerTestBase<DeweyNumber> {

	@Override
	protected TypeSerializer<DeweyNumber> createSerializer() {
		return DeweyNumberSerializer.INSTANCE;
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<DeweyNumber> getTypeClass() {
		return DeweyNumber.class;
	}

	@Override
	protected DeweyNumber[] getTestData() {
		return new DeweyNumber[] {
				DeweyNumber.fromString("358"),
				DeweyNumber.fromString("17.0.0.12.32.0.1.1"),
				DeweyNumber.fromString("1.1.1.1")
		};
	}
}
