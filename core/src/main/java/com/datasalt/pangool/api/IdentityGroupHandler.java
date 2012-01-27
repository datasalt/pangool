package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.io.tuple.ITuple;

public class IdentityGroupHandler extends GroupHandler<ITuple, NullWritable> {

	@Override
	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context,
	    Collector collector) throws IOException, InterruptedException, CoGrouperException {

		for(ITuple tuple : tuples) {
			collector.write(tuple, NullWritable.get());
		}
	}
}
