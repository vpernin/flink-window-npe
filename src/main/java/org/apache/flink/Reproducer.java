package org.apache.flink;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

/**
 * Produce NPE :
 Exception in thread "main" org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
	 at org.apache.flink.runtime.jobmanager.JobManager$$anonfun$handleMessage$1$$anonfun$applyOrElse$6.apply$mcV$sp(JobManager.scala:900)
	 at org.apache.flink.runtime.jobmanager.JobManager$$anonfun$handleMessage$1$$anonfun$applyOrElse$6.apply(JobManager.scala:843)
	 at org.apache.flink.runtime.jobmanager.JobManager$$anonfun$handleMessage$1$$anonfun$applyOrElse$6.apply(JobManager.scala:843)
	 at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
	 at scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
	 at akka.dispatch.TaskInvocation.run(AbstractDispatcher.scala:40)
	 at akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:397)
	 at scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)
	 at scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)
	 at scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)
	 at scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)
 Caused by: java.lang.NullPointerException
	 at org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger.onEventTime(ContinuousEventTimeTrigger.java:81)
	 at org.apache.flink.streaming.runtime.operators.windowing.WindowOperator$Context.onEventTime(WindowOperator.java:721)
	 at org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.onEventTime(WindowOperator.java:425)
	 at org.apache.flink.streaming.api.operators.HeapInternalTimerService.advanceWatermark(HeapInternalTimerService.java:276)
	 at org.apache.flink.streaming.api.operators.AbstractStreamOperator.processWatermark(AbstractStreamOperator.java:858)
	 at org.apache.flink.streaming.runtime.io.StreamInputProcessor.processInput(StreamInputProcessor.java:168)
	 at org.apache.flink.streaming.runtime.tasks.OneInputStreamTask.run(OneInputStreamTask.java:63)
	 at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:272)
	 at org.apache.flink.runtime.taskmanager.Task.run(Task.java:655)
	 at java.lang.Thread.run(Thread.java:745)
 */
public class Reproducer implements Serializable {

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws Exception {
		new Reproducer().run();
	}

	private void run() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		env
			// Reduce the sleep duration and it won't fail
			.addSource(getSourceFunction(env.getConfig(), 100))
			.returns(new TypeHint<Tuple2<Long, String>>(){}.getTypeInfo())
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
				@Override
				public long extractAscendingTimestamp(Tuple2<Long, String> event) {
					return event.f0;
				}
			})

			.keyBy(new KeySelector<Tuple2<Long,String>, String>() {
				@Override
				public String getKey(Tuple2<Long, String> value) throws Exception {
					return value.f1;
				}
			})
			.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.HOURS)))
			.trigger(ContinuousEventTimeTrigger.of(Time.of(10, TimeUnit.MINUTES)))
			.allowedLateness(Time.hours(2))

			.apply(
					(s, window, input, out) ->
							out.collect("Window: " + window + ", count: " + StreamSupport.stream(input.spliterator(), false).count())
					,
					BasicTypeInfo.STRING_TYPE_INFO
			)
			.print();

		env.execute();
	}

	private FromElementsFunction getSourceFunction(ExecutionConfig config, long wait) throws IOException {
		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[2];
		fieldSerializers[0] = BasicTypeInfo.LONG_TYPE_INFO.createSerializer(config);
		fieldSerializers[1] = BasicTypeInfo.STRING_TYPE_INFO.createSerializer(config);

		return new FromElementsFunction<>(
				new SlowTupleSerializer(tupleTypeInfo.getTypeClass(), fieldSerializers, wait),
				fromDate("2017-03-04 04:51:04"),
				fromDate("2017-03-03 18:11:02"),
				fromDate("2017-03-03 12:51:02"),
				fromDate("2017-03-03 21:51:04"),
				fromDate("2017-03-04 02:11:00"));
	}

	private Tuple2<Long, String> fromDate(String date) {
		try {
			return Tuple2.of(DATE_FORMAT.parse(date).getTime(), "A_CODE");
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

}
