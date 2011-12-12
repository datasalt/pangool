package com.datasalt.pangolin.grouper.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleImpl;

public abstract class GrouperMapperHandler<INPUT_KEY,INPUT_VALUE> {
	
	private Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>.Context context;
	private NullWritable outputValue = NullWritable.get();
	
	void setContext(Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>.Context context){
		this.context = context;
	}
	
	public void setup(Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>.Context context) throws IOException,InterruptedException {
		
	}
	
	public void cleanup(Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>.Context context) throws IOException,InterruptedException {
		
	}
	
	public void map(INPUT_KEY key,INPUT_VALUE value) throws IOException, InterruptedException{
		
	}
	
	protected void emit(Tuple tuple) throws IOException, InterruptedException{
		context.write(tuple, outputValue);
	}

	
	
	
}
