package com.datasalt.pangolin.grouper.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangolin.grouper.io.Tuple;

public abstract class GrouperReducerHandler<OUTPUT_KEY,OUTPUT_VALUE> {
	
	private Reducer<Tuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE>.Context context;
	
	
	void setContext(Reducer<Tuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE>.Context context){
		this.context = context;
	}
	
	public void setup(Reducer<Tuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE>.Context context) throws IOException,InterruptedException {
		
	}
	
	public void cleanup(Reducer<Tuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE>.Context context) throws IOException,InterruptedException {
		
	}
	
	public abstract void onOpenGroup(int depth,String field,Tuple firstElement) throws IOException,InterruptedException;
	public abstract void onCloseGroup(int depth,String field,Tuple lastElement) throws IOException,InterruptedException;
	public abstract void onGroupElements(Iterable<Tuple> tuples) throws IOException,InterruptedException;
	
	
	protected void emit(OUTPUT_KEY outputKey,OUTPUT_VALUE outputValue) throws IOException, InterruptedException{
		context.write(outputKey, outputValue);
	}
	
}
