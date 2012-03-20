package com.datasalt.pangool.flow;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.TupleMapper;

public class TupleInput implements RichInput {

	Schema intermediateSchema;
	TupleMapper processor;

	public TupleInput(Schema intermediateSchema) {
		this(intermediateSchema, new IdentityTupleMapper());
	}
	
	public TupleInput(Schema intermediateSchema, TupleMapper processor) {
	  this.intermediateSchema = intermediateSchema;
	  this.processor = processor;
	}
}
