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

package org.apache.flink.runtime.io.disk;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;

import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.TestMemoryAllocator;
import org.apache.flink.types.StringValue;
import org.junit.Test;


public class FileChannelStreamsTest {

	@Test
	public void testCloseAndDeleteOutputView() {
		final IOManager ioManager = new IOManagerAsync();
		try {
			TestMemoryAllocator alloc = new TestMemoryAllocator(4, 20*1024);
			FileIOChannel.ID channel = ioManager.createChannel();
			BlockChannelWriter writer = ioManager.createBlockChannelWriter(channel);
			
			FileChannelOutputView out = new FileChannelOutputView(writer, alloc, 4);
			new StringValue("Some test text").write(out);
			
			// close for the first time, make sure all memory returns
			out.close();
			assertTrue(alloc.allSegmentsAvailable());
			
			// close again, should not cause an exception
			out.close();
			
			// delete, make sure file is removed
			out.closeAndDelete();
			assertFalse(new File(channel.getPath()).exists());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			ioManager.shutdown();
		}
	}
	
	@Test
	public void testCloseAndDeleteInputView() {
		final IOManager ioManager = new IOManagerAsync();
		try {
			TestMemoryAllocator alloc = new TestMemoryAllocator(4, 20*1024);
			FileIOChannel.ID channel = ioManager.createChannel();
			
			// add some test data
			{
				FileWriter wrt = new FileWriter(channel.getPath());
				wrt.write("test data");
				wrt.close();
			}
			
			BlockChannelReader reader = ioManager.createBlockChannelReader(channel);
			FileChannelInputView in = new FileChannelInputView(reader, alloc, 4, 9);
			
			// read just something
			in.readInt();
			
			// close for the first time, make sure all memory returns
			in.close();
			assertTrue(alloc.allSegmentsAvailable());
			
			// close again, should not cause an exception
			in.close();
			
			// delete, make sure file is removed
			in.closeAndDelete();
			assertFalse(new File(channel.getPath()).exists());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			ioManager.shutdown();
		}
	}
}
