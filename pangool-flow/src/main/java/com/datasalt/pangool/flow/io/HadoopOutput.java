package com.datasalt.pangool.flow.io;

import org.apache.hadoop.mapreduce.OutputFormat;

@SuppressWarnings("rawtypes")
public class HadoopOutput implements RichOutput {
	
	final private Class key;
	final private Class value;
	final private OutputFormat outputFormat;
	
	public HadoopOutput(OutputFormat outputFormat, Class key, Class value) {
		this.key = key;
		this.value = value;
		this.outputFormat = outputFormat;
	}

	public Class getKey() {
  	return key;
  }

	public Class getValue() {
  	return value;
  }

	public OutputFormat getOutputFormat() {
  	return outputFormat;
  }
}
