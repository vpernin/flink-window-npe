package org.apache.flink;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

/**
 * Produce NPE :
 * Exception in thread "main" org.apache.flink.runtime.client.JobExecutionException: Job execution failed.
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


	public static void main(String[] args) throws Exception {
		new Reproducer().run();
	}

	private void run() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		env
			.fromParallelCollection(new DataSupplier.EventsIterator(), new TypeHint<Tuple2<Long, String>>(){}.getTypeInfo())
			.setParallelism(8)
			.map(e -> {
				// Comment the sleep and it won't fail
				TimeUnit.MILLISECONDS.sleep(10);
				return e;
			})
			.returns(new TypeHint<Tuple2<Long, String>>(){}.getTypeInfo())
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, String>>(Time.minutes(10)) {
				@Override
				public long extractTimestamp(Tuple2<Long, String> event) {
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
			.trigger(ContinuousEventTimeTrigger.of(Time.of(30, TimeUnit.SECONDS)))
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

}
