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

import com.datasalt.pangool.flow.Mutator;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;

/**
 * A special operation that implements a 1-N Tuple join. It copies all the fields from the left tuple and them all the
 * fields from the right tuple. If the 1 side of the relationship is not present, it applies the default values defined
 * in the Map<String, Object> that is given as a parameter. The joint schema has to be given as well.
 */
@SuppressWarnings("serial")
public class OneToNJoinOp extends TupleReduceOp {

	private Schema leftSchema;
	private Schema rightSchema;
	private Map<String, Object> defaultValues; // this is not a Tuple because it might be a part of a schema

	public OneToNJoinOp(Schema leftSchema, Schema rightSchema, Map<String, Object> defaultValues) {
		super(Mutator.jointSchema(leftSchema, rightSchema));
		this.leftSchema = leftSchema;
		this.rightSchema = rightSchema;
		this.defaultValues = defaultValues;
	}

	@Override
	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

		int count = 0;

		for(ITuple tuple : tuples) {
			if(tuple.getSchema().getName().equals(leftSchema.getName())) {
				if(count == 0) {
					for(Field field : leftSchema.getFields()) {
						this.tuple.set(field.getName(), tuple.get(field.getName()));
					}
				} else {
					throw new IOException("Expected to receive one tuple from left schema [" + leftSchema.getName()
					    + "] but received more than one!");
				}
			} else {
				if(count == 0) {
					for(Map.Entry<String, Object> mapEntry : defaultValues.entrySet()) {
						this.tuple.set(mapEntry.getKey(), mapEntry.getValue());
					}
				}
			}
			
			if(tuple.getSchema().getName().equals(rightSchema.getName())) {
				// Apply join operation
				for(Field field : rightSchema.getFields()) {
					this.tuple.set(field.getName(), tuple.get(field.getName()));
				}
				callback.onReturn(this.tuple);
			}
			count++;
		}
	}
}
