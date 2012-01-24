package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.io.tuple.ITuple;

public class IdentityGroupHandler extends GroupHandler<ITuple, NullWritable> {

	@Override
	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, State state,
	    ReduceContext<ITuple, NullWritable, ITuple, NullWritable> context) throws IOException, InterruptedException,
	    CoGrouperException {

		for(ITuple tuple : tuples) {
			context.write(tuple, NullWritable.get());
		}
	}
}
