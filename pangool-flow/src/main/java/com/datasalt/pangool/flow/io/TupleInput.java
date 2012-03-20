package com.datasalt.pangool.flow.io;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.TupleMapper;

@SuppressWarnings("rawtypes")
public class TupleInput implements RichInput {

	final private Schema intermediateSchema;
	final private TupleMapper processor;

	public TupleInput(Schema intermediateSchema) {
		this(intermediateSchema, new IdentityTupleMapper());
	}
	
	public TupleInput(Schema intermediateSchema, TupleMapper processor) {
	  this.intermediateSchema = intermediateSchema;
	  this.processor = processor;
	}

	public Schema getIntermediateSchema() {
  	return intermediateSchema;
  }

	public TupleMapper getProcessor() {
  	return processor;
  }
}
