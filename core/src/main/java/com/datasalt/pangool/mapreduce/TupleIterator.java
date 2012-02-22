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

package com.datasalt.pangool.mapreduce;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.DatumWrapper;

/**
 * Iterator used in {@link Grouper},specially in {@link RollupReducer}. Basically it translates an {@link Iterable} containing 
 * {@link NullWritable} objects to one that contains {@link ITuple} ones. 
 * In order to do so it handles the {@link ReduceContext} and uses {@link ReduceContext#getCurrentKey()} to obtain the key in 
 * every iteration.
 * 
 * See {@link Iterable} and {@link ITuple}
 *  
 */
public class TupleIterator<OUTPUT_KEY,OUTPUT_VALUE> implements Iterator<ITuple>, Iterable<ITuple>{

	private Iterator<NullWritable> iterator;
	private ReduceContext<DatumWrapper<ITuple>,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context;
		
	public TupleIterator(ReduceContext<DatumWrapper<ITuple>,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context){
		this.context = context;
	}
	
	public void setIterator(Iterator<NullWritable> iterator){
		this.iterator = iterator;
	}	
		
	@Override
  public boolean hasNext() {
		return iterator.hasNext();
  }

	@Override
  public ITuple next() {
			iterator.next(); //advances one key
			return context.getCurrentKey().datum();
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
