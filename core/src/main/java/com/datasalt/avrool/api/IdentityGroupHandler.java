package com.datasalt.avrool.api;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.avrool.CoGrouperException;

public class IdentityGroupHandler extends GroupHandler<Record, NullWritable> {

	@Override
	public void onGroupElements(Record group, Iterable<Record> tuples, CoGrouperContext<Record, NullWritable> context,
	    Collector<Record, NullWritable> collector) throws IOException, InterruptedException, CoGrouperException {

		for(Record tuple : tuples) {
			collector.write(tuple, NullWritable.get());
		}
	}
}
