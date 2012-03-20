package com.datasalt.pangool.flow;

import com.datasalt.pangool.io.Schema;

public class TupleOutput implements RichOutput {

	Schema outputSchema;

	public TupleOutput(Schema outputSchema) {
		this.outputSchema = outputSchema;
	}
}
