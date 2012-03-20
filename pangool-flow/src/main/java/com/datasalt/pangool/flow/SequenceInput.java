package com.datasalt.pangool.flow;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

public class SequenceInput extends HadoopInput {

	public SequenceInput(TupleMapper processor, Schema intermediateSchema) {
	  super(new HadoopInputFormat(SequenceFileInputFormat.class), processor, intermediateSchema);
  }
}
