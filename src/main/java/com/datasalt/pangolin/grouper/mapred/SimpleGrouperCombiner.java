package com.datasalt.pangolin.grouper.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;

import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleSortComparator;



public abstract class SimpleGrouperCombiner extends SimpleGrouperReducer<Tuple,NullWritable>{
	
	private Context context;
	private NullWritable outputValue = NullWritable.get();
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		this.context = context;
		((TupleSortComparator)WritableComparator.get(Tuple.class)).setConf(context.getConfiguration());
	}
	
	protected void emit(Tuple tuple) throws IOException,InterruptedException {
		context.write(tuple,outputValue);
	}

}