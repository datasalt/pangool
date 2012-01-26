package com.datasalt.avrool.api;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.io.tuple.ITuple;

public class IdentityGroupHandler extends GroupHandler<ITuple, NullWritable> {

	@Override
	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext<ITuple, NullWritable> context,
	    Collector<ITuple, NullWritable> collector) throws IOException, InterruptedException, CoGrouperException {

		for(ITuple tuple : tuples) {
			collector.write(tuple, NullWritable.get());
		}
	}
}
