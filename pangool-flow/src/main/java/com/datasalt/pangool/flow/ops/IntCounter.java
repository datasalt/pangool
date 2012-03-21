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

/**
 * Operation that sums one field from a Tuple into an Integer value.
 */
@SuppressWarnings("serial")
public class IntCounter extends Op<Iterable<ITuple>, Integer> {

	Integer count;
	String field;
	
	public IntCounter(String field) {
		this.field = field;
	}
	
	public void process(Iterable<ITuple> tuples, ReturnCallback<Integer> callback) {
		count = 0;
		for(ITuple tuple: tuples) {
			count += (Integer) tuple.get(field);
		}
		callback.onReturn(count);
	}
}
