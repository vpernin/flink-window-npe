package org.apache.flink;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.SplittableIterator;

public class Reproducer implements Serializable {

	private static final String CODE = "CODE";

	public static void main(String[] args) throws Exception {
		new Reproducer().run();
	}

	private void run() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		Map<Integer, List<String>> events = DataSupplier.get();

		env.fromParallelCollection(new EventsIterator(events), TypeInformation.of(Event.class))
		.setParallelism(8)
		.map(e -> {
			TimeUnit.MILLISECONDS.sleep(10);
			return e;
		})
		.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.minutes(10)) {
			@Override
			public long extractTimestamp(Event element) {
				return element.getDate();
			}
		})
		.keyBy(Event::getCode)
		.window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.HOURS)))
		.trigger(ContinuousEventTimeTrigger.of(Time.of(30, TimeUnit.SECONDS)))
		.allowedLateness(Time.hours(2))

		.fold(
			new Tuple1<>(0L), (FoldFunction<Event, Tuple1<Long>>) (accumulator, event) -> {
				accumulator.f0 += 1;
				return accumulator;
			}, (WindowFunction<Tuple1<Long>, Tuple3<String, Long, Long>, String, TimeWindow>) (key, window, accumulators, out) -> {
				Long count = accumulators.iterator().next().f0;
				out.collect(new Tuple3<>(key, window.getEnd(), count));
			},
			new TypeHint<Tuple1<Long>>(){}.getTypeInfo(),
			new TypeHint<Tuple3<String, Long, Long>>(){}.getTypeInfo()
		)
		.print();

		env.execute();
	}

	class Event implements Serializable {

		private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		private long date;
		private String code;

		public Event(String date) {
			try {
				this.date = dateFormat.parse(date).getTime();
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
			this.code = CODE;
		}

		public long getDate() {
			return date;
		}

		public void setDate(long date) {
			this.date = date;
		}

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}
	}

	class EventsIterator extends SplittableIterator<Event> {

		private Map<Integer, List<String>> events;

		public EventsIterator(Map<Integer, List<String>> events) {
			this.events = events;
		}

		@Override
		public Iterator<Event>[] split(int numPartitions) {
			Iterator<Event>[] iterators = (Iterator<Event>[]) new Iterator<?>[numPartitions];
			for (int i = 0; i < numPartitions; i++) {
				iterators[i] = events.get(i + 1).stream().map(date -> new Event(date)).collect(Collectors.toList()).iterator();
			}
			return iterators;
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return 8;
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Event next() {
			return null;
		}
	}

	public static class DataSupplier {

		public static Map<Integer, List<String>> get() {
			List list1 = new ArrayList<>();
			list1.add("2017-02-28 19:01:06");
			list1.add("2017-03-01 07:11:01");
			list1.add("2017-03-01 18:11:07");
			list1.add("2017-03-02 06:11:04");
			list1.add("2017-03-02 23:21:04");
			list1.add("2017-03-03 10:01:02");
			list1.add("2017-03-01 12:11:01");
			list1.add("2017-03-03 19:41:08");
			list1.add("2017-03-04 03:31:02");
			list1.add("2017-03-01 17:11:02");
			list1.add("2017-03-02 02:41:04");
			list1.add("2017-03-03 09:31:04");
			list1.add("2017-03-03 14:31:03");
			list1.add("2017-03-02 15:01:00");
			list1.add("2017-03-03 18:21:03");
			list1.add("2017-03-03 05:40:59");
			list1.add("2017-03-04 05:01:03");
			list1.add("2017-03-03 07:31:08");
			list1.add("2017-03-03 16:41:03");
			list1.add("2017-03-03 22:41:04");

			List list2 = new ArrayList<>();
			list2.add("2017-02-28 20:31:06");
			list2.add("2017-03-01 07:41:01");
			list2.add("2017-03-02 17:21:05");
			list2.add("2017-03-01 22:51:05");
			list2.add("2017-03-02 08:31:07");
			list2.add("2017-03-03 02:01:07");
			list2.add("2017-03-03 10:31:06");
			list2.add("2017-03-01 12:21:06");
			list2.add("2017-03-03 20:41:08");
			list2.add("2017-03-02 13:41:04");
			list2.add("2017-03-01 18:21:03");
			list2.add("2017-03-02 03:51:04");
			list2.add("2017-03-03 10:11:02");
			list2.add("2017-03-03 16:31:08");
			list2.add("2017-03-03 19:21:08");
			list2.add("2017-03-02 13:11:02");
			list2.add("2017-03-03 04:21:05");
			list2.add("2017-03-03 05:01:00");
			list2.add("2017-03-04 06:51:01");
			list2.add("2017-03-04 01:11:09");

			List list3 = new ArrayList<>();
			list3.add("2017-02-28 23:11:07");
			list3.add("2017-03-01 08:01:01");
			list3.add("2017-03-01 23:11:03");
			list3.add("2017-03-02 09:11:07");
			list3.add("2017-03-02 18:51:03");
			list3.add("2017-03-03 10:41:02");
			list3.add("2017-03-01 12:41:01");
			list3.add("2017-03-03 03:41:03");
			list3.add("2017-03-03 21:11:08");
			list3.add("2017-03-01 20:21:05");
			list3.add("2017-03-02 06:21:07");
			list3.add("2017-03-03 10:51:01");
			list3.add("2017-03-03 16:51:03");
			list3.add("2017-03-03 20:21:08");
			list3.add("2017-03-04 07:51:10");
			list3.add("2017-03-03 05:51:02");
			list3.add("2017-03-04 06:21:01");

			List list4 = new ArrayList<>();
			list4.add("2017-02-28 13:31:04");
			list4.add("2017-02-28 23:51:02");
			list4.add("2017-03-01 09:01:04");
			list4.add("2017-03-02 00:11:03");
			list4.add("2017-03-03 11:31:03");
			list4.add("2017-03-01 12:51:04");
			list4.add("2017-03-02 18:31:02");
			list4.add("2017-03-02 13:51:02");
			list4.add("2017-03-03 21:31:09");
			list4.add("2017-03-01 20:41:03");
			list4.add("2017-03-02 07:51:05");
			list4.add("2017-03-03 11:41:02");
			list4.add("2017-03-04 04:31:02");
			list4.add("2017-03-02 12:01:04");
			list4.add("2017-03-03 23:01:04");
			list4.add("2017-03-02 22:51:03");
			list4.add("2017-03-03 04:01:04");
			list4.add("2017-03-03 03:01:04");

			List list5 = new ArrayList<>();
			list5.add("2017-02-28 14:41:02");
			list5.add("2017-02-28 17:21:07");
			list5.add("2017-03-01 00:01:03");
			list5.add("2017-03-01 09:21:04");
			list5.add("2017-03-02 00:21:01");
			list5.add("2017-03-03 12:21:02");
			list5.add("2017-03-02 20:21:01");
			list5.add("2017-03-03 22:01:04");
			list5.add("2017-03-02 20:31:00");
			list5.add("2017-03-04 03:51:01");
			list5.add("2017-03-01 21:31:03");
			list5.add("2017-03-02 09:01:05");
			list5.add("2017-03-02 22:21:02");
			list5.add("2017-03-03 13:01:02");
			list5.add("2017-03-03 02:31:02");
			list5.add("2017-03-04 00:41:05");
			list5.add("2017-03-03 18:51:07");

			List list6 = new ArrayList<>();
			list6.add("2017-02-28 17:41:04");
			list6.add("2017-03-01 02:21:03");
			list6.add("2017-03-01 09:51:02");
			list6.add("2017-03-02 11:51:00");
			list6.add("2017-03-02 18:21:01");
			list6.add("2017-03-02 01:11:04");
			list6.add("2017-03-02 19:41:03");
			list6.add("2017-03-03 14:41:08");
			list6.add("2017-03-02 17:51:07");
			list6.add("2017-03-02 12:51:01");
			list6.add("2017-03-03 23:21:04");
			list6.add("2017-03-03 06:21:03");
			list6.add("2017-03-02 21:31:05");
			list6.add("2017-03-02 00:41:05");
			list6.add("2017-03-02 09:41:05");
			list6.add("2017-03-02 11:01:02");
			list6.add("2017-03-03 13:21:07");
			list6.add("2017-03-03 01:21:05");
			list6.add("2017-03-02 22:11:00");
			list6.add("2017-03-03 00:31:00");
			list6.add("2017-03-04 08:01:02");
			list6.add("2017-03-03 19:11:08");

			List list7 = new ArrayList<>();
			list7.add("2017-02-28 17:51:04");
			list7.add("2017-03-01 05:11:03");
			list7.add("2017-03-02 11:31:02");
			list7.add("2017-03-02 17:01:04");
			list7.add("2017-03-02 03:01:04");
			list7.add("2017-03-03 16:21:03");
			list7.add("2017-03-02 18:11:06");
			list7.add("2017-03-04 00:21:04");
			list7.add("2017-03-01 16:01:06");
			list7.add("2017-03-02 01:21:04");
			list7.add("2017-03-02 10:01:05");
			list7.add("2017-03-02 21:51:02");
			list7.add("2017-03-02 22:41:04");
			list7.add("2017-03-03 14:11:02");
			list7.add("2017-03-03 01:31:02");
			list7.add("2017-03-03 17:41:02");
			list7.add("2017-03-03 01:51:00");
			list7.add("2017-03-04 06:01:03");
			list7.add("2017-03-03 03:51:01");
			list7.add("2017-03-03 04:11:03");
			list7.add("2017-03-03 21:21:09");

			List list8 = new ArrayList<>();
			list8.add("2017-02-28 18:41:06");
			list8.add("2017-03-01 06:51:04");
			list8.add("2017-03-03 08:01:03");
			list8.add("2017-03-01 18:01:04");
			list8.add("2017-03-02 04:11:09");
			list8.add("2017-03-02 20:01:04");
			list8.add("2017-03-01 11:51:03");
			list8.add("2017-03-04 01:31:05");
			list8.add("2017-03-04 03:11:00");
			list8.add("2017-03-01 17:01:07");
			list8.add("2017-03-02 01:31:08");
			list8.add("2017-03-02 22:01:05");
			list8.add("2017-03-03 14:21:03");
			list8.add("2017-03-04 04:51:04");
			list8.add("2017-03-03 18:11:02");
			list8.add("2017-03-02 16:41:03");
			list8.add("2017-03-03 07:11:03");
			list8.add("2017-03-03 12:51:02");
			list8.add("2017-03-03 21:51:04");
			list8.add("2017-03-04 02:11:00");
			list8.add("2017-03-04 08:51:02");

			Map<Integer, List<String>> data = new HashMap<>();
			data.put(1, list1);
			data.put(2, list2);
			data.put(3, list3);
			data.put(4, list4);
			data.put(5, list5);
			data.put(6, list6);
			data.put(7, list7);
			data.put(8, list8);

			return data;
		}
	}
}
