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

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;

/**
 * Base class for implementing {@link Op}s that promote Tuples. This way we can cache the Tuple.
 */
@SuppressWarnings("serial")
public abstract class TupleOp<K> extends Op<K, ITuple> {

	protected Tuple tuple;
	private Schema schema;
	
	public TupleOp() {
		
	}
	
	public TupleOp(Schema schema) {
		this.tuple = new Tuple(schema);
		this.schema = schema;
	}
	
	// Getters
	public Tuple getTuple() {
  	return tuple;
  }

	public Schema getSchema() {
  	return schema;
	}
}
