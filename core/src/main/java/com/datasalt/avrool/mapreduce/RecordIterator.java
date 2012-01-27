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

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.GroupHandlerProxyRecord;
import com.datasalt.avrool.api.GroupHandler;

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

	private Iterator<AvroValue> iterator;
	private ReduceContext<AvroKey,AvroValue,OUTPUT_KEY,OUTPUT_VALUE> context;
	private GroupHandlerProxyRecord proxyRecord;
	/**
	 *  used to mark that the first element from the {@link Iterator} was already consumed.
	 *  This prevents calling {@link Iterator#next()} twice for the first element.
	 */
	private boolean firstTupleConsumed=false;
	
	public RecordIterator(ReduceContext<AvroKey,AvroValue,OUTPUT_KEY,OUTPUT_VALUE> context,CoGrouperConfig grouperConfig){
		this.context = context;
		
		this.proxyRecord = new GroupHandlerProxyRecord(grouperConfig);
		
	}
	
	public void setIterator(Iterator<AvroValue> iterator){
		this.iterator = iterator;
	}	
	
	/**
	 *  This is used to mark that the first element from iterable was already consumed, so in next iteration don't call iterator.next().
	 *  Instead of this reuse the currentKey in {@link ReduceContext#getCurrentKey()} 
	 *  
	 *  This method is usually called before {@link GroupHandler#onGroupElements(Iterable,Context)}
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
  public GenericRecord next() {
		try{
			if (firstTupleConsumed){
				firstTupleConsumed = false;
			} else {
				iterator.next(); //advances one key
			}
			proxyRecord.setContainedRecord((GenericRecord)context.getCurrentKey().datum());
			return proxyRecord;
		} catch(CoGrouperException e){
			throw new RuntimeException(e);
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
	public Iterator<GenericRecord> iterator() {
		return this;
	}

}
