package com.datasalt.pangool.flow;

import org.apache.hadoop.mapreduce.OutputFormat;

public class HadoopOutput implements RichOutput {
	
	Class key;
	Class value;
	OutputFormat outputFormat;
	
	public HadoopOutput(OutputFormat outputFormat, Class key, Class value) {
		this.key = key;
		this.value = value;
		this.outputFormat = outputFormat;
	}
}
