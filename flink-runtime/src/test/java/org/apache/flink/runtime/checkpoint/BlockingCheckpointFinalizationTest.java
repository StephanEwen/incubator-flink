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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;

import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTest.mockExecutionVertex;

import static org.junit.Assert.*;

/**
 * Test validates that the checkpoint coordinator does not get stuck when the
 * finalization of a pending checkpoint to a final checkpoint (which writes out
 * the metadata) blocks.
 */
public class BlockingCheckpointFinalizationTest {

	private static final String BLOCK_FS_SCHEME = "blockfs";

	private final ExecutorService executor = Executors.newCachedThreadPool();

	@After
	public void shutdown() {
		executor.shutdownNow();
	}

	// ------------------------------------------------------------------------

	@Test
	public void testFinalizeCheckpointWithBlockingStream() throws Exception {
		prepareBlockFs();

		// the path to which writing will block 
		final String externalPath = BLOCK_FS_SCHEME + "://some/ignored/path";

		// the single task with which we simulate acknowledging the checkpoint 
		final JobID jobID = new JobID();
		final ExecutionAttemptID task = new ExecutionAttemptID();

		final CheckpointCoordinator coord = createCheckpointCoordinator(jobID, externalPath, executor, task);

		coord.triggerCheckpoint(System.currentTimeMillis(), false);
		assertEquals(1, coord.getNumberOfPendingCheckpoints());

		final long checkpointId = coord.getPendingCheckpoints().values().iterator().next().getCheckpointId(); 

		coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jobID, task, checkpointId));
	}

	private CheckpointCoordinator createCheckpointCoordinator(
			JobID jobId,
			String externalizationPath,
			Executor ioExecutor,
			ExecutionAttemptID... tasks)
	{
		ExecutionVertex[] vertices = new ExecutionVertex[tasks.length];
		for (int i = 0; i < vertices.length; i++) {
			vertices[i] = mockExecutionVertex(tasks[i]);
		}

		return new CheckpointCoordinator(
				jobId,
				100000000000L, // base checkpoint interval
				100000000000L, // checkpointTimeout
				0L,            // minPauseBetweenCheckpoints
				1,             //maxConcurrentCheckpointAttempts
				ExternalizedCheckpointSettings.externalizeCheckpoints(true),
				vertices, vertices, vertices,
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				externalizationPath,
				ioExecutor);
	}

	private void prepareBlockFs() throws Exception {
		final Field fsMapField = FileSystem.class.getDeclaredField("FSDIRECTORY");
		fsMapField.setAccessible(true);

		@SuppressWarnings("unchecked")
		final Map<String, String> fsMap = (Map<String, String>) fsMapField.get(null);

		fsMap.put(BLOCK_FS_SCHEME, BlockingFileSystem.class.getName());
	}
	
	// ------------------------------------------------------------------------
	//  test file system
	// ------------------------------------------------------------------------

	/**
	 * A mean stream that blocks un-interruptibly in all methods until it is closed.
	 */
	private static class BlockingStream extends FSDataOutputStream {

		private final Object blocker = new Object();

		private boolean closed;

		@Override
		public long getPos() {
			return 0;
		}

		@Override
		public void write(int b) {
			block();
		}

		@Override
		public void flush() {
			block();
		}

		@Override
		public void sync() {
			block();
		}

		@Override
		public void close() {
			synchronized (blocker) {
				closed = true;
				blocker.notifyAll();
			}
		}

		private void block() {
			// We wait especially badly here, suppressing interrupts while the stream is not closed.
			// This mimics the behavior of some badly behaving FS libraries, including
			// certain versions of HDFS and S3

			synchronized (blocker) {
				while (!closed) {
					try {
						blocker.wait();
					}
					catch (InterruptedException ignored) {}
				}
			}
		}
	}

	/**
	 * A file system that creates blocking streams
	 */
	public static class BlockingFileSystem extends FileSystem {

		private static final URI uri = URI.create(BLOCK_FS_SCHEME + ":/");

		@Override
		public void initialize(URI name) {}

		@Override
		public URI getUri() {
			return uri;
		}

		@Override
		public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
			return new BlockingStream();
		}

		@Override
		public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException {
			return create(f, overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE);
		}

		@Override
		public FileStatus getFileStatus(Path f) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public FSDataInputStream open(Path f, int bufferSize) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public FSDataInputStream open(Path f) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public FileStatus[] listStatus(Path f) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean delete(Path f, boolean recursive) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean mkdirs(Path f) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean rename(Path src, Path dst) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Path getWorkingDirectory() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Path getHomeDirectory() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isDistributedFS() {
			return true;
		}
	}
}
