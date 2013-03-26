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

package com.datasalt.pangool.tuplemr.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;

/**
 * Iterator used in {@link SimpleReducer} and {@link RollupReducer}.
 * <p>
 * 
 * Basically it translates an {@link Iterable} containing {@link NullWritable}
 * objects to one that contains {@link ITuple} objects. In order to do so it
 * handles the {@link ReduceContext} and uses {@link ReduceContext#getCurrentKey()} 
 * to obtain the key in every iteration.
 * 
 * @see Iterable
 * @see ITuple
 * 
 */
public class TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> implements Iterator<ITuple>,
    Iterable<ITuple> {

	private Iterator<NullWritable> iterator;
	private ReduceContext<DatumWrapper<ITuple>, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> context;

	public TupleIterator(
	    ReduceContext<DatumWrapper<ITuple>, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> context) {
		this.context = context;
	}

	public void setIterator(Iterator<NullWritable> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public ITuple next() {
		iterator.next(); // advances one key
		try {
	    return context.getCurrentKey().datum();
    } catch(Throwable e) {
    	// catching Throwable here because in Hadoop 1.0 there is no exception thrown
    	// but there are signed exceptions in Hadoop 2.0.
	    throw new RuntimeException(e);
    }
	}

	@Override
	public void remove() {
		iterator.remove();
	}

	@Override
	public Iterator<ITuple> iterator() {
		return this;
	}
}
