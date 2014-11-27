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

package org.apache.flink.runtime.io.network.api;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.deployment.ChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.BufferRecycler;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.io.network.channels.InputChannel;
import org.apache.flink.runtime.io.network.gates.GateID;
import org.apache.flink.runtime.io.network.gates.InputGate;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.types.IntValue;


public class InterruptedReadTest {
	
	private static final BufferRecycler NOOP_RECYCLER = new BufferRecycler() {
		@Override
		public void recycle(MemorySegment buffer) {}
	};
	
	@Test
	public void testMutableReaderReleasesThreadOnInterrupt() {
		try {
			final JobID jobId = new JobID();
			
			InputGate<IOReadableWritable> ig = new InputGate<IOReadableWritable>(jobId, new GateID(), 0);
			ig.initializeChannels(createGateDeploymentDescriptor());
			InputChannel<IOReadableWritable> channel = ig.channels()[0];
			
			Environment env = mock(Environment.class);
			when(env.createAndRegisterInputGate()).thenReturn(ig);
			AbstractInvokable invokable = mock(AbstractInvokable.class);
			when(invokable.getEnvironment()).thenReturn(env);
			
			final MutableRecordReader<IOReadableWritable> reader = new MutableRecordReader<IOReadableWritable>(invokable);
			
			final AtomicBoolean boolRef = new AtomicBoolean();
			final AtomicReference<Throwable> errorRef = new AtomicReference<Throwable>();
			final Object lock = new Object();
			
			Runnable readRunnable = new Runnable() {
				
				@Override
				public void run() {
					try {
						int last = 0;
						try {
							while (true) {
								IntValue next = new IntValue();
								assertTrue(reader.next(next));
								
								int value = next.getValue();
								assertTrue(value == 0 || value == last+1);
								last = value;
								
								// the first reading loop is slow
								Thread.sleep(1);
							}
						}
						catch (InterruptedException e) {
							// the first one, now we go without delay
						}
						
						while (true) {
							IntValue next = new IntValue();
							assertTrue(reader.next(next));
							
							int value = next.getValue();
							assertTrue(value == 0 || value == last+1);
							last = value;
						}
						
					}
					catch (InterruptedException e) {
						synchronized (lock) {
							boolRef.set(true);
							lock.notifyAll();
						}
					}
					catch (Throwable t) {
						synchronized (lock) {
							errorRef.set(t);
							lock.notifyAll();
						}
					}
				}
			};
			
			Thread readThread = new Thread(readRunnable, "read thread");
			readThread.start();
			
			// feed some data to the reader
			Envelope env1 = new Envelope(0, jobId, new ChannelID());
			env1.setBuffer(createBuffer());
			channel.queueEnvelope(env1);
			Envelope env2 = new Envelope(1, jobId, new ChannelID());
			env2.setBuffer(createBuffer());
			channel.queueEnvelope(env2);
			
			// wait a bit that the reader can read a few records
			Thread.sleep(20);
			
			// interrupt for the first time, in the middle of a buffer
			readThread.interrupt();
			
			// give the reader time to finish reading
			Thread.sleep(20);
			
			// interrupt for the second time, after the buffers
			readThread.interrupt();
			
			synchronized (lock) {
				while (!boolRef.get() && errorRef.get() == null) {
					lock.wait();
				}
			}
			
			if (errorRef.get() != null) {
				Throwable t = errorRef.get();
				if (t instanceof Error) {
					throw (Error) t;
				} else if (t instanceof Exception) {
					throw (Exception) t;
				} else {
					t.printStackTrace();
					fail(t.getMessage());
				}
			}
			
			// the marker that the interrupt worked
			assertTrue(boolRef.get());
			
			readThread.join(2000);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private GateDeploymentDescriptor createGateDeploymentDescriptor() {
		ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor(new ChannelID(), new ChannelID());
		return new GateDeploymentDescriptor(Collections.singletonList(cdd));
	}
	
	private Buffer createBuffer() {
		MemorySegment mem = new MemorySegment(new byte[1024]);
		for (int i = 0; i < 100; i++) {
			mem.putIntBigEndian(8*i, 4);
			mem.putIntBigEndian(8*i + 4, i);
		}
		
		return new Buffer(mem, 800, NOOP_RECYCLER);
	}
}
