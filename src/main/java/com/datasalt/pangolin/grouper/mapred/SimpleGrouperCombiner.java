package com.datasalt.pangolin.grouper.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangolin.grouper.io.ITuple;



public abstract class SimpleGrouperCombiner extends SimpleGrouperReducer<ITuple,NullWritable>{
	
	//private Context context;
	//private NullWritable outputValue = NullWritable.get();
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		//this.context = context;
		
	}
	
//	protected void emit(ITuple tuple) throws IOException,InterruptedException {
//		context.write(tuple,outputValue);
//	}

}