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

package org.apache.flink.streaming.recipes;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction.Context;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction.OnTimerContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.utils.ThrottledSource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.UUID;

/**
 * <h1>Join with Timeout</h1>
 *
 * <p>This simple recipe illustrates how to join two streams of events with a timeout on the events
 * in one stream: Events from stream A are received and wait for a certain time (t) for a matching
 * event from stream B.
 * If a corresponding event from stream B comes within that time, the event is immediately sent out.
 * If NO corresponding event from stream B comes within that time, the original event is sent to a
 * special stream for timeout elements.
 *
 * <p>The sample use case for this recipe is a stream of transactions that await approval.
 * When a transaction event is received, it is held in state, waiting for the corresponding
 * approval event. Transactions and approval events are matched by transaction ID.
 * If the approval event is received in time, the transaction is put into the result stream.
 * If the approval does not come in time, the transaction is put into a different stream, for
 * timed out transactions.
 *
 * <p>To extend this example a bit, the approval event can in fact also be a disapproval event,
 * in which case the transaction event is sent to yet another stream (rejected transactions).
 *
 * <h3>The Recipe</h3>
 *
 * <p>The main part of this recipe is the {@link JoinFunction}, an implementation of the
 * {@link CoProcessFunction} that implements the logic to join the two data streams and observe
 * the timeout. The JoinFunction maintains the pending transactions in a ValueState "txn".
 * The state is organized by key, meaning it always automatically bound to the current key,
 * which is here the transaction ID. The JoinFunction must consequently run on keyed streams.
 *
 * <p>The function receives transactions on its first input: {@code processElement1(...)}. When a
 * transaction event is received, it is stored in a ValueState "txn". In addition
 * to storing the transaction event, the {@code processElement1(...)} method also sets a timeout
 * timer.
 *
 * <p>The function receives approval events on its second input: {@code processElement2(...)}.
 * For a received approval event, it fetches the transaction event from the state and emits it
 * through its main output (if the approval event is a proper approval) or to the side output
 * with the "rejected" tag (if the approval event is a rejection).
 *
 * <p>When the timeout timer fires, it calls the {@link JoinFunction#onTimer(long, OnTimerContext, Collector)}
 * method. That method checks the value state if it still contains the transaction, which
 * indicates that the transaction is still pending. In that case, it removes the transaction event
 * from state and side-outputs the transaction event with the "timeout" tag. Note that the
 * timer also triggers if the transaction was already handled on time, but the state will be
 * empty in that case and the timer will be ignored. It is simpler to just let the timer trigger
 * and ignore it than explicitly cancelling the timer.
 *
 * <p>The method {@link #timeoutJoin(DataStream, DataStream)} connects the streams with the
 * transaction events and the approval events, calls the JoinFunction, and obtains the three
 * resulting streams: the main output stream with the approved transactions, and the two side
 * output streams with the tags for rejected transactions and timed-out transactions.
 * The stream flow of this recipe is illustrated below.
 *
 * <pre>
 *  [ Transactions Stream] --> (keyBy) -+                        +--{side output: rejected transactions}-->
 *                                      |                        |
 *                                      +--> (join with timeout)-+--{main output: approved transactions}-->
 *                                      |                        |
 *  [  Approvals Stream  ] --> (keyBy) -+                        +--{side output: timeout transactions }-->
 * </pre>
 *
 * <h3>A Note on Time</h3>
 *
 * <p>The pattern in this recipe works for both <i>event time</i> and <i>processing time</i>.
 * The implementation in this class uses processing time, but its simple to use this with event time
 * by changing the timer call in {@link JoinFunction#processElement1(Transaction, Context, Collector)}
 * to register an event time timer instead, and activating event time and watermarking in the application.
 *
 * <h3>A Note on Order</h3>
 *
 * <p>This recipe makes the simplifying assumption that transaction events are strictly received
 * before their matching approval events. If that is not strictly the case, approval events for
 * which no transaction event exists yet, would need to be stored as well (with a timeout).
 * When a transaction event is received, the JoinFunction would check if there is already an
 * approval event.
 */
@SuppressWarnings("serial")
public class JoinWithTimeout {

	// ------------------------------------------------------------------------
	//  The Timeout-Join Recipe
	// ------------------------------------------------------------------------

	/** The tag that marks the stream for rejected transactions. */
	private static final OutputTag<Transaction> REJECT_STREAM = new OutputTag<Transaction>("reject"){};

	/** The tag that marks the stream with the transactions that timed out. */
	private static final OutputTag<Transaction> TIMEOUT_STREAM = new OutputTag<Transaction>("timeout"){};

	/**
	 * Takes the stream of transaction events and the stream of approval (or rejection) events
	 * and joins them with a timeout on the transaction events. The method returns three streams:
	 * The stream with approved transactions, the stream with rejected transactions, and the
	 * stream with transactions that timed out.
	 *
	 * @param txnStream The stream of transaction events.
	 * @param approvalStream The stream of approval (or rejection) events.
	 *
	 * @return The three result streams.
	 */
	public static ResultStreams timeoutJoin(DataStream<Transaction> txnStream, DataStream<ApproveOrReject> approvalStream) {

		// join the streams with the approvals using our join process function
		SingleOutputStreamOperator<Transaction> join =
				txnStream.keyBy(Transaction::getTxnId)
						.connect(approvalStream.keyBy(ApproveOrReject::getTxnId))
						.process(new JoinFunction()).name("Txn/Approvals Join");

		return new ResultStreams(
				join, // the main output holds the approved transactions
				join.getSideOutput(REJECT_STREAM),
				join.getSideOutput(TIMEOUT_STREAM));
	}

	/**
	 * The function that implements the join between the transactions and the approvals.
	 */
	public static class JoinFunction extends CoProcessFunction<Transaction, ApproveOrReject, Transaction> {

		/** The timeout for transactions waiting for approval. */
		private static final int TIMEOUT_MILLIS = 2000;

		/** The state in which the transaction is stored while awaiting the
		 * matching approval/rejection. */
		private ValueState<Transaction> pendingTransaction;

		/**
		 * This method is called for each transaction.
		 */
		@Override
		public void processElement1(Transaction txn, Context ctx, Collector<Transaction> out) throws Exception {
			// keep the transaction in the internal state until the approval comes
			pendingTransaction.update(txn);

			// schedule a timer to trigger the timeout
			ctx.timerService().registerProcessingTimeTimer(txn.getTimestamp() + TIMEOUT_MILLIS);
		}

		/**
		 * This method is called for each approval / rejection.
		 */
		@Override
		public void processElement2(ApproveOrReject approval, Context ctx, Collector<Transaction> out) throws Exception {
			Transaction txn = pendingTransaction.value();

			if (txn != null) {
				// output the transaction to the main stream (approved) or a side stream (reject)
				if (approval.isApproved()) {
					out.collect(txn);
				} else {
					ctx.output(REJECT_STREAM, txn);
				}

				// remove the transaction from the state
				pendingTransaction.clear();
			}
		}

		/**
		 * This method is called by the timer when a transaction times out.
		 */
		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Transaction> out) throws Exception {
			Transaction txn = pendingTransaction.value();

			// check if the transaction is still there, in which case it would be timed out
			if (txn != null) {
				// write to the timeout stream
				ctx.output(TIMEOUT_STREAM, txn);

				// clear the state
				pendingTransaction.clear();
			}
		}

		/**
		 * Initialization method called at the beginning.
		 */
		@Override
		public void open(Configuration parameters) {
			// get access to the state that caches the transactions
			pendingTransaction = getRuntimeContext().getState(
					new ValueStateDescriptor<>("txn", Transaction.class));
		}
	}

	/** A simple 3-tuple of data streams, for approved, rejected, and timed-out transactions. */
	public static class ResultStreams {

		public final DataStream<Transaction> approved;
		public final DataStream<Transaction> rejected;
		public final DataStream<Transaction> timedOut;

		public ResultStreams(DataStream<Transaction> approved, DataStream<Transaction> rejected, DataStream<Transaction> timedOut) {
			this.approved = approved;
			this.rejected = rejected;
			this.timedOut = timedOut;
		}
	}

	// ------------------------------------------------------------------------
	//  End-of-Recipe
	//
	//  Everything below here is an example program to let you see the
	//  recipe in action
	// ------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	//  Sample data types used in this recipe
	// ------------------------------------------------------------------------

	/**
	 * Data type representing a transaction.
	 */
	public static class Transaction {

		private final UUID txnId;

		private final long timestamp;

		private final String txnPayload;

		public Transaction(UUID txnId, long timestamp, String txnPayload) {
			this.txnId = txnId;
			this.timestamp = timestamp;
			this.txnPayload = txnPayload;
		}

		public UUID getTxnId() {
			return txnId;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return "Transaction " + txnId + " @ "
					+ FORMATTER.get().format(timestamp) + " : " + txnPayload;
		}

		private static final ThreadLocal<SimpleDateFormat> FORMATTER = ThreadLocal.withInitial(
				() -> new SimpleDateFormat("dd.MM.yyyy HH:mm:ss:SSS"));
	}

	/**
	 * Data type representing a decision to approve or reject the transaction.
	 */
	public static class ApproveOrReject {

		private final UUID txnId;

		private final boolean approved;

		public ApproveOrReject(UUID txnId, boolean approved) {
			this.txnId = txnId;
			this.approved = approved;
		}

		public UUID getTxnId() {
			return txnId;
		}

		public boolean isApproved() {
			return approved;
		}

		@Override
		public String toString() {
			return txnId + (approved ? " : approve" : " : reject");
		}
	}

	// ------------------------------------------------------------------------
	//  Sample Application based on the Recipe
	// ------------------------------------------------------------------------

	/**
	 * Main entry point to the sample application.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataStream<Transaction> txnStream = SampleDataGenerator.getTransactions(env);

		DataStream<ApproveOrReject> approvalStream = SampleDataGenerator.getApprovals(env);

		ResultStreams resultStreams = timeoutJoin(txnStream, approvalStream);

		// print the results to std out
		resultStreams.approved.map((v) -> "Approve: " + v).print();
		resultStreams.rejected.map((v) -> "Reject: " + v).print();
		resultStreams.timedOut.map((v) -> "Time out: " + v).print();

		// start the execution
		env.execute();
	}

	// ------------------------------------------------------------------------
	//  Sample Data Generation
	// ------------------------------------------------------------------------

	/**
	 * Simple sample data generator.
	 *
	 * <p>The generator creates transactions and matching approvals by running two pseudo-random
	 * number generators from the same random seed. That way, both produce the same sequence of
	 * transaction IDs.
	 *
	 * <p>The data generator also produces at an artificially throttled data rate to make it easier
	 * to read console output and understand what is happening.
	 *
	 * <p>Note that the source generators here are NOT fault tolerant, meaning on failure/recovery
	 * they reset.
	 */
	private static class SampleDataGenerator {

		private static final long RND_SEED = 765726439756234523L;

		private static final Random TXN_RND = new Random(RND_SEED);

		private static final Random APPR_RND = new Random(RND_SEED);

		/** The . Increase this if you want to see more throughput. */
		private static final int RECORDS_PER_SECOND = 5;

		/**
		 * Creates a stream with random Transaction events.
		 */
		public static DataStream<Transaction> getTransactions(StreamExecutionEnvironment env) {
			SourceFunction<Transaction> generator =
					new ThrottledSource<>(new TransactionsGenerator(), Transaction.class, RECORDS_PER_SECOND);

			return env.addSource(generator).name("Transactions Generator").rebalance();
		}

		/**
		 * Creates a stream with random Approval / Rejection events.
		 */
		public static DataStream<ApproveOrReject> getApprovals(StreamExecutionEnvironment env) {
			SourceFunction<ApproveOrReject> generator =
					new ThrottledSource<>(new ApprovalsGenerator(), ApproveOrReject.class, RECORDS_PER_SECOND);

			return env.addSource(generator).name("Approvals Generator").rebalance();
		}

		/**
		 * The transactions generator generates random Transaction events. It uses the same sequence
		 * of pseudo-random numbers as the ApprovalsGenerator to make sure that transactions and
		 * approvals match up.
		 */
		private static class TransactionsGenerator implements Iterator<Transaction>, Serializable {

			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public Transaction next() {
				long id1 = TXN_RND.nextLong();
				long id2 = TXN_RND.nextLong();

				// draw another number to keep the transactions generator and the
				// approval generator in sync
				TXN_RND.nextInt();

				return new Transaction(new UUID(id1, id2), System.currentTimeMillis(), "(payload)");
			}
		}

		/**
		 * The ApprovalsGenerator generates the same transactions as the transactions generator, but
		 * delays sending out the approval / rejection by a random time.
		 */
		private static class ApprovalsGenerator implements Iterator<ApproveOrReject>, Serializable {

			/** The queue holds the generated approvals / rejections should be sent out after a
			 * small delay. */
			private final PriorityQueue<EventWithTimestamp> queue = new PriorityQueue<>();

			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public ApproveOrReject next() {
				// generate the next event
				long id1 = APPR_RND.nextLong();
				long id2 = APPR_RND.nextLong();
				int delay = Math.abs(APPR_RND.nextInt() % 1350);

				UUID txnId = new UUID(id1, id2);

				if (delay < 1000) {
					// approved, emit with delay
					long emitAt = System.currentTimeMillis() + delay;
					queue.add(new EventWithTimestamp(new ApproveOrReject(txnId, true), emitAt));
				} else if (delay < 1200) {
					// reject, emit with delay
					long emitAt = System.currentTimeMillis() + delay / 2;
					queue.add(new EventWithTimestamp(new ApproveOrReject(txnId, false), emitAt));
				}

				// emit the next event, if the delay for the next event has expired
				EventWithTimestamp head = queue.peek();
				if (head != null && head.timestamp < System.currentTimeMillis()) {
					queue.poll();
					return head.event;
				} else {
					return null;
				}
			}

			/** Combination of an approval/rejection with a timestamp, to be stored in the delay queue. */
			private static class EventWithTimestamp implements Comparable<EventWithTimestamp> {

				final ApproveOrReject event;
				final long timestamp;

				EventWithTimestamp(ApproveOrReject event, long timestamp) {
					this.event = event;
					this.timestamp = timestamp;
				}

				@Override
				public int compareTo(EventWithTimestamp o) {
					return Long.compare(timestamp, o.timestamp);
				}
			}
		}
	}
}
