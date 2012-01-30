package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.io.PangoolMultipleOutputs;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MultipleOutputsCollector {

  PangoolMultipleOutputs multipleOutputs;
	
	public MultipleOutputsCollector(MapContext context) {
		multipleOutputs = new PangoolMultipleOutputs(context);
	}
	
	public MultipleOutputsCollector(ReduceContext context) {
		multipleOutputs = new PangoolMultipleOutputs(context);
	}
	
	public <K, V> RecordWriter<K, V> getNamedOutput(String namedOutput) throws IOException, InterruptedException {
		return multipleOutputs.getRecordWriter(namedOutput);
	}
	
	public <K, V>void write(String namedOutput, K key, V value) throws IOException, InterruptedException {
		multipleOutputs.write(namedOutput, key, value);
	}
	
	public void close() throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
