/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorCheckpointSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SourceCoordinatorProvider}.
 */
@SuppressWarnings("serial")
public class SourceCoordinatorProviderTest {
	private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);
	private static final int NUM_SPLITS = 10;

	private SourceCoordinatorProvider<MockSourceSplit> provider;

	@Before
	public void setup() {
		provider = new SourceCoordinatorProvider<>(
				"SourceCoordinatorProviderTest",
				OPERATOR_ID,
				new MockSource(Boundedness.BOUNDED, NUM_SPLITS),
				1);
	}

	@Test
	public void testCreate() throws Exception {
		OperatorCoordinator coordinator =
				provider.create(new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SPLITS));
		assertTrue(coordinator instanceof RecreateOnResetOperatorCoordinator);
	}

	@Test
	public void testCheckpointAndReset() throws Exception {
		final OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SPLITS);
		final RecreateOnResetOperatorCoordinator coordinator =
				(RecreateOnResetOperatorCoordinator) provider.create(context);
		final SourceCoordinator<?, ?> sourceCoordinator =
				(SourceCoordinator<?, ?>) coordinator.getInternalCoordinator();

		// Start the coordinator.
		coordinator.start();
		// register reader 0 and take a checkpoint.
		coordinator.handleEventFromOperator(0, new ReaderRegistrationEvent(0, "location"));
		CompletableFuture<byte[]> future = new CompletableFuture<>();
		coordinator.checkpointCoordinator(0L, future);
		byte[] bytes = future.get();

		// Register reader 1.
		coordinator.handleEventFromOperator(1, new ReaderRegistrationEvent(1, "location"));
		// Wait until the coordinator context is updated with registration of reader 1.
		while (sourceCoordinator.getContext().registeredReaders().size() < 2) {
			Thread.sleep(1);
		}

		// reset the coordinator to the checkpoint which only contains reader 0.
		coordinator.resetToCheckpoint(bytes);
		final SourceCoordinator<?, ?> restoredSourceCoordinator =
				(SourceCoordinator<?, ?>) coordinator.getInternalCoordinator();
		assertNotEquals("The restored source coordinator should be a different instance",
				restoredSourceCoordinator, sourceCoordinator);
		assertEquals("There should only be one registered reader.",
				1, restoredSourceCoordinator.getContext().registeredReaders().size());
		assertNotNull("The only registered reader should be reader 0",
				restoredSourceCoordinator.getContext().registeredReaders().get(0));
	}

	@Test
	public void testUserClassLoaderWhenCreatingNewEnumerator() throws Exception {
		final ClassLoader testClassLoader = new URLClassLoader(new URL[0]);
		final EnumeratorCreatingSource<?, ClassLoaderTestEnumerator> source =
				new EnumeratorCreatingSource<>(ClassLoaderTestEnumerator::new);

		final OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), testClassLoader);
		final SourceCoordinatorProvider<?> provider = new SourceCoordinatorProvider<>(
				"testOperator", context.getOperatorId(), source, 1);

		final OperatorCoordinator coordinator = provider.create(context);
		coordinator.start();

		final ClassLoaderTestEnumerator enumerator = source.createEnumeratorFuture.get();
		assertSame(testClassLoader, enumerator.constructorClassLoader);
		assertSame(testClassLoader, enumerator.threadClassLoader.get());

		// cleanup
		coordinator.close();
	}

	@Test
	public void testUserClassLoaderWhenRestoringEnumerator() throws Exception {
		final ClassLoader testClassLoader = new URLClassLoader(new URL[0]);
		final EnumeratorCreatingSource<?, ClassLoaderTestEnumerator> source =
			new EnumeratorCreatingSource<>(ClassLoaderTestEnumerator::new);

		final OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), testClassLoader);
		final SourceCoordinatorProvider<?> provider = new SourceCoordinatorProvider<>(
			"testOperator", context.getOperatorId(), source, 1);

		final OperatorCoordinator coordinator = provider.create(context);
		coordinator.resetToCheckpoint(createEmptyCheckpoint(1L));
		coordinator.start();

		final ClassLoaderTestEnumerator enumerator = source.restoreEnumeratorFuture.get();
		assertSame(testClassLoader, enumerator.constructorClassLoader);
		assertSame(testClassLoader, enumerator.threadClassLoader.get());

		// cleanup
		coordinator.close();
	}

	// ------------------------------------------------------------------------
	//  test helpers
	// ------------------------------------------------------------------------

	private static byte[] createEmptyCheckpoint(long checkpointId) throws Exception {
		try (SourceCoordinatorContext<MockSourceSplit> emptyContext = new SourceCoordinatorContext<>(
				Executors.newDirectExecutorService(),
				new SourceCoordinatorProvider.CoordinatorExecutorThreadFactory("test", SourceCoordinatorProviderTest.class.getClassLoader()),
				1,
				new MockOperatorCoordinatorContext(new OperatorID(), 0),
				new MockSourceSplitSerializer())) {

			return SourceCoordinator.writeCheckpointBytes(
					checkpointId,
					Collections.emptySet(),
					emptyContext,
					new MockSplitEnumeratorCheckpointSerializer(),
					new MockSourceSplitSerializer());
		}
	}


	// ------------------------------------------------------------------------
	//  test mocks
	// ------------------------------------------------------------------------

	private static final class ClassLoaderTestEnumerator implements SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> {

		final CompletableFuture<ClassLoader> threadClassLoader = new CompletableFuture<>();
		final ClassLoader constructorClassLoader;

		public ClassLoaderTestEnumerator() {
			this.constructorClassLoader = Thread.currentThread().getContextClassLoader();
		}

		@Override
		public void start() {
			threadClassLoader.complete(Thread.currentThread().getContextClassLoader());
		}

		@Override
		public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void addSplitsBack(List<MockSourceSplit> splits, int subtaskId) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void addReader(int subtaskId) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Set<MockSourceSplit> snapshotState() throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() {}
	}

	private static final class EnumeratorCreatingSource<T, EnumT extends SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>>>
			implements Source<T, MockSourceSplit, Set<MockSourceSplit>> {

		final CompletableFuture<EnumT> createEnumeratorFuture = new CompletableFuture<>();
		final CompletableFuture<EnumT> restoreEnumeratorFuture = new CompletableFuture<>();
		private final Supplier<EnumT> enumeratorFactory;

		public EnumeratorCreatingSource(Supplier<EnumT> enumeratorFactory) {
			this.enumeratorFactory = enumeratorFactory;
		}

		@Override
		public Boundedness getBoundedness() {
			return Boundedness.CONTINUOUS_UNBOUNDED;
		}

		@Override
		public SourceReader<T, MockSourceSplit> createReader(SourceReaderContext readerContext) {
			throw new UnsupportedOperationException();
		}

		@Override
		public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> createEnumerator(
				SplitEnumeratorContext<MockSourceSplit> enumContext) {
			final EnumT enumerator = enumeratorFactory.get();
			createEnumeratorFuture.complete(enumerator);
			return enumerator;
		}

		@Override
		public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> restoreEnumerator(
				SplitEnumeratorContext<MockSourceSplit> enumContext,
				Set<MockSourceSplit> checkpoint) {
			final EnumT enumerator = enumeratorFactory.get();
			restoreEnumeratorFuture.complete(enumerator);
			return enumerator;
		}

		@Override
		public SimpleVersionedSerializer<MockSourceSplit> getSplitSerializer() {
			return new MockSourceSplitSerializer();
		}

		@Override
		public SimpleVersionedSerializer<Set<MockSourceSplit>> getEnumeratorCheckpointSerializer() {
			return new MockSplitEnumeratorCheckpointSerializer();
		}
	}
}
