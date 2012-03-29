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

import com.datasalt.pangool.flow.Utils;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;

/**
 * Operation that shallow-copies one Tuple's content to another. It caches the destination tuple and accepts a "copy"
 * schema.
 */
@SuppressWarnings("serial")
public class Copy extends TupleOp<ITuple> {

	Tuple tuple;
	Schema copySchema;

	public Copy(Schema schema, Schema copySchema) {
		super(schema);
		this.copySchema = copySchema;
		this.tuple = new Tuple(schema);
	}

	@Override
	public void process(ITuple input, ReturnCallback<ITuple> callback) throws IOException, InterruptedException {
		Utils.shallowCopy(input, tuple, copySchema);
		callback.onReturn(tuple);
	}
}
