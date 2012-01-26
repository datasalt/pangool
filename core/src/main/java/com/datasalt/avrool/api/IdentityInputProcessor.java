package com.datasalt.avrool.api;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.io.NullWritable;

public class IdentityInputProcessor extends InputProcessor<Record, NullWritable> {

	@Override
	public void process(Record key, NullWritable value, CoGrouperContext context, Collector collector)
	    throws IOException, InterruptedException {
		collector.write(key);
	}
}
