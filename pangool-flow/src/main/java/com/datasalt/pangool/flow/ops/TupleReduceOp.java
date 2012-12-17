package com.datasalt.pangool.flow.ops;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

@SuppressWarnings("serial")
public abstract class TupleReduceOp extends TupleOp<Iterable<ITuple>> {

	public TupleReduceOp() {
		
	}
	
	public TupleReduceOp(Schema schema) {
	  super(schema);
  }

}
