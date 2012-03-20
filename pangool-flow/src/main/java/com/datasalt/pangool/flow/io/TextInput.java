package com.datasalt.pangool.flow.io;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

@SuppressWarnings("rawtypes")
public class TextInput extends HadoopInput {

	public TextInput(TupleMapper processor, Schema intermediateSchema) {
	  super(new HadoopInputFormat(TextInputFormat.class), processor, intermediateSchema);
  }
}
