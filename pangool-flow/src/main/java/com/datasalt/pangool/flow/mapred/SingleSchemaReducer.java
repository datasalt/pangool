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
package com.datasalt.pangool.flow.mapred;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleReducer;

/**
 * This reducer can be used for implementing custom reducers that will emit one Schema type. This way a Tuple instance can
 * be cached and reused.
 */
@SuppressWarnings("serial")
public abstract class SingleSchemaReducer<T, K> extends TupleReducer<T, K> {

	private final Tuple outTuple;
	private final Schema outputSchema;
	
	public SingleSchemaReducer(Schema outputSchema) {
		this.outputSchema = outputSchema;
		this.outTuple = new Tuple(outputSchema);
	}
	
	protected ITuple getOutputTuple(){
		return outTuple;
	}
	
	protected Schema getOutputSchema(){
		return outputSchema;
	}
}
