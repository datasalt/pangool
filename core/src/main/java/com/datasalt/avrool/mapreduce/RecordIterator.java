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

package com.datasalt.avrool.mapreduce;

import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.PangoolKey;
import com.datasalt.avrool.io.records.ReducerProxyRecord;

/**
 * Iterator used in {@link Grouper},specially in {@link RollupReducer}. Basically it translates an {@link Iterable} containing 
 * {@link NullWritable} objects to one that contains {@link ITuple} ones. 
 * In order to do so it handles the {@link ReduceContext} and uses {@link ReduceContext#getCurrentKey()} to obtain the key in 
 * every iteration.
 * 
 * See {@link Iterable} and {@link ITuple}
 *  
 * 
 * 
 */
public class RecordIterator<OUTPUT_KEY,OUTPUT_VALUE> implements Iterator<GenericRecord>, Iterable<GenericRecord>{

	private Iterator<NullWritable> iterator;
	private ReduceContext<PangoolKey,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context;
	private ReducerProxyRecord proxyRecord;
	/**
	 *  used to mark that the first element from the {@link Iterator} was already consumed.
	 *  This prevents calling {@link Iterator#next()} twice for the first element.
	 */
	public RecordIterator(ReduceContext<PangoolKey,NullWritable,OUTPUT_KEY,OUTPUT_VALUE> context,CoGrouperConfig grouperConfig){
		this.context = context;
		
		this.proxyRecord = new ReducerProxyRecord(grouperConfig);
		
	}
	
	public void setIterator(Iterator<NullWritable> iterator){
		this.iterator = iterator;
	}	
	
	@Override
  public boolean hasNext() {
			return iterator.hasNext();
  }

	@Override
  public GenericRecord next() {
		try{
				iterator.next(); //advances one key internally
				proxyRecord.setContainedRecord((GenericRecord)context.getCurrentKey().datum());
				return proxyRecord;
		} catch(CoGrouperException e){
			throw new RuntimeException(e);
		}
  }

	@Override
  public void remove() {
			iterator.remove();
  }

	@Override
	public Iterator<GenericRecord> iterator() {
		return this;
	}

}
