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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * A source reader thread factory that helps
 */
public class SourceReaderThreadFactory implements ThreadFactory {
	private static final Logger LOG = LoggerFactory.getLogger(SourceReaderThreadFactory.class);
	private final Consumer<Throwable> exceptionConsumer;

	SourceReaderThreadFactory(Consumer<Throwable> exceptionConsumer) {
		this.exceptionConsumer = exceptionConsumer;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r, "SourceFetcher");
		t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				LOG.error("Caught unhandled exception in the source reader", e);
				exceptionConsumer.accept(e);
			}
		});
		return t;
	}
}
