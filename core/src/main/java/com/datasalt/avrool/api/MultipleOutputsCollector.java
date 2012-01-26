package com.datasalt.avrool.api;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputsPatched;

/**
 * 
 * @author pere
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MultipleOutputsCollector {

  MultipleOutputsPatched multipleOutputs;
	
	public MultipleOutputsCollector(Mapper.Context context) {
		multipleOutputs = new MultipleOutputsPatched(context);
	}
	
	public MultipleOutputsCollector(Reducer.Context context) {
		multipleOutputs = new MultipleOutputsPatched(context);
	}
	
	public <T, K> RecordWriter<T, K> getNamedOutput(String namedOutput) throws IOException, InterruptedException {
		return multipleOutputs.getRecordWriter(namedOutput);
	}
}
