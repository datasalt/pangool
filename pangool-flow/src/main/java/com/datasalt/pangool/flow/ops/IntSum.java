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

/**
 * Operation that sums a certain integer field from a group of Tuples and sets the result of the sum into a certain field.
 */
@SuppressWarnings("serial")
public class IntSum extends TupleReduceOp {

	private String origField;
	private String destField;
	
	public IntSum(String field) {
		this(field, field);
	}
	
	/**
	 * The destination field is the same than the field we use to sum.
	 */
	public IntSum(String origField, String destField) {
		this.origField = origField;
		this.destField = destField;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  int count = 0;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count += (Integer) tuple.get(origField);
			lastTuple = tuple;
		}
		lastTuple.set(destField, count);
		callback.onReturn(lastTuple);
	}
}
