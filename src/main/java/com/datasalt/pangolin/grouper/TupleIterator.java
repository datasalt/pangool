/**
 * Copyright [2011] [Datasalt Systems S.L.]
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

package com.datasalt.pangolin.grouper;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.pangolin.grouper.io.ITuple;
import com.datasalt.pangolin.grouper.io.Tuple;

/**
 * Iterator used in {@link Grouper} and {@link Grouper}. Basically it translates an {@link Iterable} containing 
 * {@link NullWritable} objects to one that contains {@link ITuple} ones. 
 * In order to do so, it handles the {@link ReduceContext} and uses {@link ReduceContext#getCurrentKey()} to obtain the key in 
 * every iteration.
 * 
 * See {@link Iterable} and {@link ITuple}
 *  
 * @author eric
 * 
 */
public class TupleIterator<OUTPUT_KEY,OUTPUT_VALUE> implements Iterator<ITuple>,Iterable<ITuple>{

	private Iterator<NullWritable> iterator;
	private ReduceContext<ITuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context;
	
	/**
	 *  used to mark that the first element from the {@link Iterator} was already consumed.
	 *  This prevents calling iterator.next() twice for the first element.
	 */
	private boolean firstTupleConsumed=false;
	
	public TupleIterator(){	}
	
	public void setIterator(Iterator<NullWritable> iterator){
		this.iterator = iterator;
	}
	
	public void setContext(ReduceContext<ITuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context){
		this.context = context;
	}
	
	
	/**
	 *  This is used to mark that the first element from iterable was already consumed, so in next iteration don't call iterator.next().
	 *  Instead of this reuse the currentKey in {@link ReduceContext#getCurrentKey()} 
	 *  
	 *  This method is usually called before {@link Grouper#onElements()}
	 */
	
	public void setFirstTupleConsumed(boolean available){
		this.firstTupleConsumed = available;
	}
	
	@Override
  public boolean hasNext() {
		if (firstTupleConsumed){
			return true;
		} else {
			return iterator.hasNext();
		}
  }

	@Override
  public ITuple next() {
		if (firstTupleConsumed){
			firstTupleConsumed = false;
			return context.getCurrentKey();
		} else {
			iterator.next(); //advances one key
			return context.getCurrentKey();
		}
  }

	@Override
  public void remove() {
		if (firstTupleConsumed){
			firstTupleConsumed = false;
		} else {
			iterator.remove();
		}
  }

	@Override
	public Iterator<ITuple> iterator() {
		return this;
	}

}
