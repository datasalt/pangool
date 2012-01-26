package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.io.PangoolMultipleOutputs;

/**
 * 
 * @author pere
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MultipleOutputsCollector {

  PangoolMultipleOutputs multipleOutputs;
	
	public MultipleOutputsCollector(Mapper.Context context) {
		multipleOutputs = new PangoolMultipleOutputs(context);
	}
	
	public MultipleOutputsCollector(Reducer.Context context) {
		multipleOutputs = new PangoolMultipleOutputs(context);
	}
	
	public <T, K> RecordWriter<T, K> getNamedOutput(String namedOutput) throws IOException, InterruptedException {
		return multipleOutputs.getRecordWriter(namedOutput);
	}
	
	public void close() throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
