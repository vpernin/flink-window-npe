package org.apache.flink;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.DataInputView;

/**
 * Mimic the reality
 */
public class SlowTupleSerializer extends TupleSerializer<Tuple2<Long, String>> {

	private final long wait;

	public SlowTupleSerializer(Class<Tuple2<Long, String>> tupleClass, TypeSerializer<?>[] fieldSerializers, long wait) {
		super(tupleClass, fieldSerializers);
		this.wait = wait;
	}

	public Tuple2<Long, String> deserialize(DataInputView source) throws IOException {
		Tuple2<Long, String> deserialize = super.deserialize(source);
		try {
			TimeUnit.MILLISECONDS.sleep(wait);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return deserialize;
	}
}