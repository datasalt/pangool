package com.datasalt.pangool.flow.io;

import org.apache.hadoop.mapreduce.InputFormat;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMapper;

@SuppressWarnings("rawtypes")
public class HadoopInput implements RichInput {

  final private InputFormat format;
  final private TupleMapper processor;
  final private Schema intermediateSchema;
	
	public HadoopInput(InputFormat format, TupleMapper processor, Schema intermediateSchema) {
		this.format = format;
	  this.processor = processor;
	  this.intermediateSchema = intermediateSchema;
	}

	public InputFormat getFormat() {
  	return format;
  }

	public TupleMapper getProcessor() {
  	return processor;
  }

	public Schema getIntermediateSchema() {
  	return intermediateSchema;
  }
}
