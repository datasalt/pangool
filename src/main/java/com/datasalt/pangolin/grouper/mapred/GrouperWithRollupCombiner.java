package com.datasalt.pangolin.grouper.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangolin.grouper.io.Tuple;



public class GrouperWithRollupCombiner extends GrouperWithRollupReducer<Tuple,NullWritable>{
		//private GrouperReducerHandler<Tuple,NullWritable> handler;
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);

	}
	
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		super.cleanup(context);
	}
	
	
}
