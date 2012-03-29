package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.io.ITuple;

@SuppressWarnings("serial")
public class Set extends Op<ITuple, ITuple> {

	String field;
	Object obj;
	
	public Set(String field, Object obj) {
		this.field = field;
		this.obj = obj;
	}
	
	@Override
  public void process(ITuple input, ReturnCallback<ITuple> callback) throws IOException, InterruptedException {
		input.set(field, obj);
		callback.onReturn(input);
  }
}
