package com.datasalt.pangool.flow.ops;

import java.io.IOException;
import java.util.Map;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

/**
 * Operation that maps some tuple schema's names to another tuple schema's names.
 * Given the Map<String, String> it perform destTuple.set(key(), origTuple.get(value()))
 */
@SuppressWarnings("serial")
public class TupleMapOp extends TupleOp<ITuple> {

	private Map<String, String> mapping;
	
	public TupleMapOp(Schema emitSchema, Map<String, String> mapping) {
	  super(emitSchema);
	  this.mapping = mapping;
  }

	@Override
  public void process(ITuple tuple, ReturnCallback<ITuple> callback) throws IOException,
      InterruptedException {
	  
		for(Map.Entry<String, String> entry: mapping.entrySet()) {
			this.tuple.set(entry.getKey(), tuple.get(entry.getValue()));
		}
		callback.onReturn(this.tuple);
  }
}
