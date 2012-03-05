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
package com.datasalt.pangool.tuplemr;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.mapred.RollupReducer;

/**
 * 
 * Similar to {@link TupleReducer} but added functionality to be used with
 * rollup
 * 
 * @see RollupReducer
 * 
 */
@SuppressWarnings("serial")
public class TupleRollupReducer<OUTPUT_KEY, OUTPUT_VALUE> extends
    TupleReducer<OUTPUT_KEY, OUTPUT_VALUE> {

	/**
	 * 
	 * This is the method called any time that a sub-group is opened when rollup
	 * is used. Check {@link TupleMRBuilder} doc about how roll-up feature works
	 * 
	 * @param depth
	 *          The tuple's field index that is currently being opened.0 when it's
	 *          the first field
	 * @param field
	 *          The tuple's field name that is currently being opened.
	 * @param firstElement
	 *          The first tuple from the current group
	 * @param context
	 *          The reducer context as in {@link Reducer}
	 * 
	 */
	public void onOpenGroup(int depth, String field, ITuple firstElement,
	    TupleMRContext context, Collector collector) throws IOException,
	    InterruptedException, TupleMRException {
	}

	/**
	 * 
	 * This is the method called after every sub-group is being closed when rollup
	 * is used. Check {@link TupleMRBuilder} doc about how roll-up feature works
	 * 
	 * @param depth
	 *          The tuple's field index that is currently being opened.It's 0 when
	 *          it's the first field
	 * @param field
	 *          The tuple's field name that is currently being opened.
	 * @param lastElement
	 *          The last tuple from the current group
	 * @param context
	 *          The reducer context as in {@link Reducer}
	 * 
	 */
	public void onCloseGroup(int depth, String field, ITuple lastElement,
	    TupleMRContext context, Collector collector) throws IOException,
	    InterruptedException, TupleMRException {

	}
}
