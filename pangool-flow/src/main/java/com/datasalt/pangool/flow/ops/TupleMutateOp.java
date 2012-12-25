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
import java.util.Map;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

/**
 * Operation that maps some tuple schema's names to another tuple schema's names.
 * Given the Map<String, String> it perform destTuple.set(key(), origTuple.get(value()))
 */
@SuppressWarnings("serial")
public class TupleMutateOp extends TupleOp<ITuple> {

	private Map<String, String> mapping;
	
	public TupleMutateOp(Schema emitSchema, Map<String, String> mapping) {
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
