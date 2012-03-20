package com.datasalt.pangool.flow.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;

@SuppressWarnings("serial")
public class StraightTopReducer extends TupleReducer<ITuple, NullWritable> {

	int n;
	
	public StraightTopReducer(int n) {
		this.n = n;
	}

	public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
	    throws IOException, InterruptedException, TupleMRException {

		Iterator<ITuple> iterator = tuples.iterator();
		for(int i = 0; i < n && iterator.hasNext(); i++) {
			collector.write(iterator.next(), NullWritable.get());
		}
	}
}
