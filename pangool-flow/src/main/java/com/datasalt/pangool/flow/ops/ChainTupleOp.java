/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
