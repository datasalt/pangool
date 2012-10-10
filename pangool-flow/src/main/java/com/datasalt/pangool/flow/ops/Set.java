package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

@SuppressWarnings("serial")
public class Set extends TupleOp<ITuple> {

	String field;
	Object obj;
	
	public Set(String field, Object obj, Schema schema) {
		super(schema);
		this.field = field;
		this.obj = obj;
	}
	
	@Override
  public void process(ITuple input, ReturnCallback<ITuple> callback) throws IOException, InterruptedException {
		input.set(field, obj);
		callback.onReturn(input);
  }
}
