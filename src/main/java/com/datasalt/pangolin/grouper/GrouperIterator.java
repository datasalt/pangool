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

/**
 * 
 * @author epalace
 *
 * @param <OUTPUT_KEY>
 * @param <OUTPUT_VALUE>
 */
public class GrouperIterator<OUTPUT_KEY,OUTPUT_VALUE> implements Iterator<Tuple>,Iterable<Tuple>{

	private Iterator<NullWritable> iterator;
	private ReduceContext<Tuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context;
	
	private boolean firstAvailable=false;
	
	public GrouperIterator(){	}
	
	public void setIterator(Iterator<NullWritable> iterator){
		this.iterator = iterator;
	}
	
	public void setContext(ReduceContext<Tuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context){
		this.context = context;
	}
	
	public void setFirstAvailable(boolean available){
		this.firstAvailable = available;
	}
	
	@Override
  public boolean hasNext() {
		if (firstAvailable){
			return true;
		} else {
			return iterator.hasNext();
		}
  }

	@Override
  public Tuple next() {
		if (firstAvailable){
			firstAvailable = false;
			return context.getCurrentKey();
		} else {
			iterator.next(); //advances one key
			return context.getCurrentKey();
		}
  }

	@Override
  public void remove() {
	  iterator.remove();
  }

	@Override
	public Iterator<Tuple> iterator() {
		return this;
	}

}
