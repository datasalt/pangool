package com.datasalt.pangool.flow.io;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

@SuppressWarnings("rawtypes")
public class SequenceInput extends HadoopInput {

	public SequenceInput(TupleMapper processor, Schema intermediateSchema) {
	  super(new HadoopInputFormat(SequenceFileInputFormat.class), processor, intermediateSchema);
  }
}
