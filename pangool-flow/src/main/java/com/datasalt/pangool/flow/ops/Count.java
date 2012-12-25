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
 * Operation that counts the number of tuples that are in a group of tuples and sets the result as a long Object in the specified tuple field.
 */
@SuppressWarnings("serial")
public class Count extends TupleReduceOp {

	String destField;
	
	public Count(String destField) {
		this.destField = destField;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  long count = 0l;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count ++;
			lastTuple = tuple;
		}
		lastTuple.set(destField, count);
		callback.onReturn(lastTuple);
	}
}