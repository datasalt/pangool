package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

/**
 * Wraps a {@link ChainOp} where the last Op is a TupleOp.
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class ChainTupleOp<K> extends TupleOp<K> {

	ChainOp<K, ITuple> chain;
	
	public ChainTupleOp(Op... ops) {
		super();
		chain = new ChainOp<K, ITuple>(ops);
  }

	@SuppressWarnings("unchecked")
  @Override
  public void process(Object input, ReturnCallback callback) throws IOException, InterruptedException {
		chain.process((K) input, callback);
	}
	
	@Override
	public Schema getSchema() {
		Schema schema = null;
		for(Op<K, ITuple> op: chain.ops) {
			if(op instanceof TupleOp) {
				if(((TupleOp)op).getSchema() != null) {
					schema = ((TupleOp)op).getSchema();
				}
			}
		}
		return schema;
	}
}
