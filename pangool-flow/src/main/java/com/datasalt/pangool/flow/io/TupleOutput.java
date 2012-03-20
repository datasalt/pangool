package com.datasalt.pangool.flow.io;

import com.datasalt.pangool.io.Schema;

public class TupleOutput implements RichOutput {

	final private Schema outputSchema;

	public TupleOutput(Schema outputSchema) {
		this.outputSchema = outputSchema;
	}

	public Schema getOutputSchema() {
  	return outputSchema;
  }
}
