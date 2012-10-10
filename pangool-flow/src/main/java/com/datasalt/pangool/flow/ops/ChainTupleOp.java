package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.io.ITuple;

/**
 * Wraps a {@link ChainOp} where the last Op is a TupleOp.
 */
public class ChainTupleOp<K> extends TupleOp {

	ChainOp<K, ITuple> chain;
	
	public ChainTupleOp(Op... ops) {
		super(((TupleOp)ops[ops.length - 1]).getSchema());
		chain = new ChainOp<K, ITuple>(ops);
  }

	@Override
  public void process(Object input, ReturnCallback callback) throws IOException, InterruptedException {
		chain.process((K) input, callback);
	}
}
