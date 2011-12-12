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

import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleImpl;

/**
 * Iterator used in {@link GrouperWithRollup} and {@link Grouper}. Basically it translates an {@link Iterable}<NullWritable> to {@link Iterable}<Tuple>. 
 * In order to do so, it handles the @{ReduceContext} and uses @{ReduceContext.getCurrentKey()} to obtain the key in 
 * every iteration.
 * 
 * See {@link Iterable} and {@link TupleImpl}
 *  
 * @author epalace
 * 
 */
public class TupleIterator<OUTPUT_KEY,OUTPUT_VALUE> implements Iterator<Tuple>,Iterable<Tuple>{

	private Iterator<NullWritable> iterator;
	private ReduceContext<? extends Tuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context;
	
	/*
	 *  used to mark that the first element from iterable was already consumed, so in next iteration don't call iterator.next().
	 */
	private boolean firstTupleConsumed=false;
	
	public TupleIterator(){	}
	
	public void setIterator(Iterator<NullWritable> iterator){
		this.iterator = iterator;
	}
	
	public void setContext(ReduceContext<? extends Tuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context){
		this.context = context;
	}
	
	
	/**
	 *  This is used to mark that the first element from iterable was already consumed, so in next iteration don't call iterator.next().
	 *  Instead of this reuse the currentKey in context.getCurrentKey(). 
	 *  This method is usually called before {@link GrouperWithRollup.onElements()}
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
  public Tuple next() {
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
	public Iterator<Tuple> iterator() {
		return this;
	}

}
