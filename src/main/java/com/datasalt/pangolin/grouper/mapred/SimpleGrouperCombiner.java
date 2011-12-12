package com.datasalt.pangolin.grouper.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;

import com.datasalt.pangolin.grouper.io.TupleImpl;
import com.datasalt.pangolin.grouper.io.TupleSortComparator;



public abstract class SimpleGrouperCombiner extends SimpleGrouperReducer<TupleImpl,NullWritable>{
	
	private Context context;
	private NullWritable outputValue = NullWritable.get();
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		this.context = context;
		((TupleSortComparator)WritableComparator.get(TupleImpl.class)).setConf(context.getConfiguration());
	}
	
	protected void emit(TupleImpl tuple) throws IOException,InterruptedException {
		context.write(tuple,outputValue);
	}

}