package com.datasalt.pangool.flow.io;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

@SuppressWarnings("rawtypes")
public class SequenceOutput extends HadoopOutput {

	public SequenceOutput(Class key, Class value) {
	  super(new HadoopOutputFormat(SequenceFileOutputFormat.class), key, value);
  }
}
