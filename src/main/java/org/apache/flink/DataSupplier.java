package org.apache.flink;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.SplittableIterator;

public class DataSupplier {

	private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	private static final String CODE = "A_CODE";

	static class EventsIterator extends SplittableIterator<Tuple2<Long, String>> {

		@Override
		public Iterator<Tuple2<Long,String>>[] split(int numPartitions) {
			Iterator<Tuple2<Long,String>>[] iterators = (Iterator<Tuple2<Long,String>>[]) new Iterator<?>[numPartitions];
			for (int i = 0; i < numPartitions; i++) {
				iterators[i] = getSampleData().get(i).stream().collect(Collectors.toList()).iterator();
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
		public Tuple2<Long,String> next() {
			return null;
		}
	}

	public static Map<Integer, List<Tuple2<Long, String>>> getSampleData() {
		List list1 = new ArrayList<>();
		list1.add(fromDate("2017-02-28 19:01:06"));
		list1.add(fromDate("2017-03-01 07:11:01"));
		list1.add(fromDate("2017-03-01 18:11:07"));
		list1.add(fromDate("2017-03-02 06:11:04"));
		list1.add(fromDate("2017-03-02 23:21:04"));
		list1.add(fromDate("2017-03-03 10:01:02"));
		list1.add(fromDate("2017-03-01 12:11:01"));
		list1.add(fromDate("2017-03-03 19:41:08"));
		list1.add(fromDate("2017-03-04 03:31:02"));
		list1.add(fromDate("2017-03-01 17:11:02"));
		list1.add(fromDate("2017-03-02 02:41:04"));
		list1.add(fromDate("2017-03-03 09:31:04"));
		list1.add(fromDate("2017-03-03 14:31:03"));
		list1.add(fromDate("2017-03-02 15:01:00"));
		list1.add(fromDate("2017-03-03 18:21:03"));
		list1.add(fromDate("2017-03-03 05:40:59"));
		list1.add(fromDate("2017-03-04 05:01:03"));
		list1.add(fromDate("2017-03-03 07:31:08"));
		list1.add(fromDate("2017-03-03 16:41:03"));
		list1.add(fromDate("2017-03-03 22:41:04"));

		List list2 = new ArrayList<>();
		list2.add(fromDate("2017-02-28 20:31:06"));
		list2.add(fromDate("2017-03-01 07:41:01"));
		list2.add(fromDate("2017-03-02 17:21:05"));
		list2.add(fromDate("2017-03-01 22:51:05"));
		list2.add(fromDate("2017-03-02 08:31:07"));
		list2.add(fromDate("2017-03-03 02:01:07"));
		list2.add(fromDate("2017-03-03 10:31:06"));
		list2.add(fromDate("2017-03-01 12:21:06"));
		list2.add(fromDate("2017-03-03 20:41:08"));
		list2.add(fromDate("2017-03-02 13:41:04"));
		list2.add(fromDate("2017-03-01 18:21:03"));
		list2.add(fromDate("2017-03-02 03:51:04"));
		list2.add(fromDate("2017-03-03 10:11:02"));
		list2.add(fromDate("2017-03-03 16:31:08"));
		list2.add(fromDate("2017-03-03 19:21:08"));
		list2.add(fromDate("2017-03-02 13:11:02"));
		list2.add(fromDate("2017-03-03 04:21:05"));
		list2.add(fromDate("2017-03-03 05:01:00"));
		list2.add(fromDate("2017-03-04 06:51:01"));
		list2.add(fromDate("2017-03-04 01:11:09"));

		List list3 = new ArrayList<>();
		list3.add(fromDate("2017-02-28 23:11:07"));
		list3.add(fromDate("2017-03-01 08:01:01"));
		list3.add(fromDate("2017-03-01 23:11:03"));
		list3.add(fromDate("2017-03-02 09:11:07"));
		list3.add(fromDate("2017-03-02 18:51:03"));
		list3.add(fromDate("2017-03-03 10:41:02"));
		list3.add(fromDate("2017-03-01 12:41:01"));
		list3.add(fromDate("2017-03-03 03:41:03"));
		list3.add(fromDate("2017-03-03 21:11:08"));
		list3.add(fromDate("2017-03-01 20:21:05"));
		list3.add(fromDate("2017-03-02 06:21:07"));
		list3.add(fromDate("2017-03-03 10:51:01"));
		list3.add(fromDate("2017-03-03 16:51:03"));
		list3.add(fromDate("2017-03-03 20:21:08"));
		list3.add(fromDate("2017-03-04 07:51:10"));
		list3.add(fromDate("2017-03-03 05:51:02"));
		list3.add(fromDate("2017-03-04 06:21:01"));

		List list4 = new ArrayList<>();
		list4.add(fromDate("2017-02-28 13:31:04"));
		list4.add(fromDate("2017-02-28 23:51:02"));
		list4.add(fromDate("2017-03-01 09:01:04"));
		list4.add(fromDate("2017-03-02 00:11:03"));
		list4.add(fromDate("2017-03-03 11:31:03"));
		list4.add(fromDate("2017-03-01 12:51:04"));
		list4.add(fromDate("2017-03-02 18:31:02"));
		list4.add(fromDate("2017-03-02 13:51:02"));
		list4.add(fromDate("2017-03-03 21:31:09"));
		list4.add(fromDate("2017-03-01 20:41:03"));
		list4.add(fromDate("2017-03-02 07:51:05"));
		list4.add(fromDate("2017-03-03 11:41:02"));
		list4.add(fromDate("2017-03-04 04:31:02"));
		list4.add(fromDate("2017-03-02 12:01:04"));
		list4.add(fromDate("2017-03-03 23:01:04"));
		list4.add(fromDate("2017-03-02 22:51:03"));
		list4.add(fromDate("2017-03-03 04:01:04"));
		list4.add(fromDate("2017-03-03 03:01:04"));

		List list5 = new ArrayList<>();
		list5.add(fromDate("2017-02-28 14:41:02"));
		list5.add(fromDate("2017-02-28 17:21:07"));
		list5.add(fromDate("2017-03-01 00:01:03"));
		list5.add(fromDate("2017-03-01 09:21:04"));
		list5.add(fromDate("2017-03-02 00:21:01"));
		list5.add(fromDate("2017-03-03 12:21:02"));
		list5.add(fromDate("2017-03-02 20:21:01"));
		list5.add(fromDate("2017-03-03 22:01:04"));
		list5.add(fromDate("2017-03-02 20:31:00"));
		list5.add(fromDate("2017-03-04 03:51:01"));
		list5.add(fromDate("2017-03-01 21:31:03"));
		list5.add(fromDate("2017-03-02 09:01:05"));
		list5.add(fromDate("2017-03-02 22:21:02"));
		list5.add(fromDate("2017-03-03 13:01:02"));
		list5.add(fromDate("2017-03-03 02:31:02"));
		list5.add(fromDate("2017-03-04 00:41:05"));
		list5.add(fromDate("2017-03-03 18:51:07"));

		List list6 = new ArrayList<>();
		list6.add(fromDate("2017-02-28 17:41:04"));
		list6.add(fromDate("2017-03-01 02:21:03"));
		list6.add(fromDate("2017-03-01 09:51:02"));
		list6.add(fromDate("2017-03-02 11:51:00"));
		list6.add(fromDate("2017-03-02 18:21:01"));
		list6.add(fromDate("2017-03-02 01:11:04"));
		list6.add(fromDate("2017-03-02 19:41:03"));
		list6.add(fromDate("2017-03-03 14:41:08"));
		list6.add(fromDate("2017-03-02 17:51:07"));
		list6.add(fromDate("2017-03-02 12:51:01"));
		list6.add(fromDate("2017-03-03 23:21:04"));
		list6.add(fromDate("2017-03-03 06:21:03"));
		list6.add(fromDate("2017-03-02 21:31:05"));
		list6.add(fromDate("2017-03-02 00:41:05"));
		list6.add(fromDate("2017-03-02 09:41:05"));
		list6.add(fromDate("2017-03-02 11:01:02"));
		list6.add(fromDate("2017-03-03 13:21:07"));
		list6.add(fromDate("2017-03-03 01:21:05"));
		list6.add(fromDate("2017-03-02 22:11:00"));
		list6.add(fromDate("2017-03-03 00:31:00"));
		list6.add(fromDate("2017-03-04 08:01:02"));
		list6.add(fromDate("2017-03-03 19:11:08"));

		List list7 = new ArrayList<>();
		list7.add(fromDate("2017-02-28 17:51:04"));
		list7.add(fromDate("2017-03-01 05:11:03"));
		list7.add(fromDate("2017-03-02 11:31:02"));
		list7.add(fromDate("2017-03-02 17:01:04"));
		list7.add(fromDate("2017-03-02 03:01:04"));
		list7.add(fromDate("2017-03-03 16:21:03"));
		list7.add(fromDate("2017-03-02 18:11:06"));
		list7.add(fromDate("2017-03-04 00:21:04"));
		list7.add(fromDate("2017-03-01 16:01:06"));
		list7.add(fromDate("2017-03-02 01:21:04"));
		list7.add(fromDate("2017-03-02 10:01:05"));
		list7.add(fromDate("2017-03-02 21:51:02"));
		list7.add(fromDate("2017-03-02 22:41:04"));
		list7.add(fromDate("2017-03-03 14:11:02"));
		list7.add(fromDate("2017-03-03 01:31:02"));
		list7.add(fromDate("2017-03-03 17:41:02"));
		list7.add(fromDate("2017-03-03 01:51:00"));
		list7.add(fromDate("2017-03-04 06:01:03"));
		list7.add(fromDate("2017-03-03 03:51:01"));
		list7.add(fromDate("2017-03-03 04:11:03"));
		list7.add(fromDate("2017-03-03 21:21:09"));

		List list8 = new ArrayList<>();
		list8.add(fromDate("2017-02-28 18:41:06"));
		list8.add(fromDate("2017-03-01 06:51:04"));
		list8.add(fromDate("2017-03-03 08:01:03"));
		list8.add(fromDate("2017-03-01 18:01:04"));
		list8.add(fromDate("2017-03-02 04:11:09"));
		list8.add(fromDate("2017-03-02 20:01:04"));
		list8.add(fromDate("2017-03-01 11:51:03"));
		list8.add(fromDate("2017-03-04 01:31:05"));
		list8.add(fromDate("2017-03-04 03:11:00"));
		list8.add(fromDate("2017-03-01 17:01:07"));
		list8.add(fromDate("2017-03-02 01:31:08"));
		list8.add(fromDate("2017-03-02 22:01:05"));
		list8.add(fromDate("2017-03-03 14:21:03"));
		list8.add(fromDate("2017-03-04 04:51:04"));
		list8.add(fromDate("2017-03-03 18:11:02"));
		list8.add(fromDate("2017-03-02 16:41:03"));
		list8.add(fromDate("2017-03-03 07:11:03"));
		list8.add(fromDate("2017-03-03 12:51:02"));
		list8.add(fromDate("2017-03-03 21:51:04"));
		list8.add(fromDate("2017-03-04 02:11:00"));
		list8.add(fromDate("2017-03-04 08:51:02"));

		Map<Integer, List<Tuple2<Long, String>>> data = new HashMap() {{
			put(0, list1);
			put(1, list2);
			put(2, list3);
			put(3, list4);
			put(4, list5);
			put(5, list6);
			put(6, list7);
			put(7, list8);
		}};

		return data;
	}

	private static Tuple2<Long, String> fromDate(String date) {
		try {
			return Tuple2.of(DATE_FORMAT.get().parse(date).getTime(), CODE);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
}