package com.datasalt.pangool.flow.io;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

public class TextOutput extends HadoopOutput {

	public TextOutput() {
	  super(new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
  }
}
