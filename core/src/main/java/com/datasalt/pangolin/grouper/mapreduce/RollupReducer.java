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

package com.datasalt.pangolin.grouper.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.TupleIterator;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.DoubleBufferPangolinTuple;
import com.datasalt.pangolin.grouper.io.tuple.GroupComparator;
import com.datasalt.pangolin.grouper.io.tuple.Partitioner;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.mapreduce.handler.GroupHandler;
import com.datasalt.pangool.mapreduce.SimpleReducer;

/**
 * 
 * This {@link Reducer} implements a similar functionality than {@link SimpleReducer} but adding a Rollup feature.
 * 
 * @author eric
 *

 */
public class RollupReducer<OUTPUT_KEY,OUTPUT_VALUE> extends Reducer<ITuple, NullWritable, OUTPUT_KEY,OUTPUT_VALUE> {

	private boolean firstIteration = true;
	private Schema schema;
	private int minDepth, maxDepth;
	private TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	private GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> handler;

	protected Schema getSchema() {
		return schema;
	}
    	
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override  	
  public void setup(Context context) throws IOException,InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			this.schema = Schema.parse(conf);
			String[] groupFields = GroupComparator.getGroupComparatorFields(conf);
			this.maxDepth = groupFields.length - 1;
			String[] partitionerFields = Partitioner.getPartitionerFields(conf);
			this.minDepth = partitionerFields.length - 1;

			this.grouperIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);
			
			Class<? extends GroupHandler> handlerClass = Grouper.getGroupHandler(conf);
			this.handler = ReflectionUtils.newInstance(handlerClass, conf);
			handler.setup(schema, context);
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
  	
  }
  
  public void cleanup(Context context) throws IOException,InterruptedException {
  	try{
  	handler.cleanup(schema,context);
  	} catch(GrouperException e){
  		throw new RuntimeException(e);
  	}
  }
  

  @Override
  public final void run(Context context) throws IOException,InterruptedException {
		try {
			setup(context);
			firstIteration = true;
			while(context.nextKey()) {
				reduce(context.getCurrentKey(), context.getValues(), context);
				((DoubleBufferPangolinTuple)context.getCurrentKey()).swapInstances();
				// TODO look if this matches super.run() implementation
			}

			// close last group
			for(int i = maxDepth; i >= minDepth; i--) {
				handler.onCloseGroup(i, schema.getFields()[i].getName(), context.getCurrentKey(), context);
			}
			cleanup(context);
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
  }
  
  
  @Override
	public final void reduce(ITuple key, Iterable<NullWritable> values, Context context) throws IOException,
	    InterruptedException {
		try {
			Iterator<NullWritable> iterator = values.iterator();
			grouperIterator.setIterator(iterator);
			iterator.next();
			DoubleBufferPangolinTuple currentKey = (DoubleBufferPangolinTuple) context.getCurrentKey();
			int indexMismatch;
			if(firstIteration) {
				indexMismatch = minDepth;
				firstIteration = false;
			} else {
				ITuple previousKey = currentKey.getPreviousTuple();
				indexMismatch = indexMismatch(previousKey, currentKey, minDepth, maxDepth);
				for(int i = maxDepth; i >= indexMismatch; i--) {
					handler.onCloseGroup(i, schema.getFields()[i].getName(), previousKey, context);
				}
			}

			for(int i = indexMismatch; i <= maxDepth; i++) {
				handler.onOpenGroup(i, schema.getFields()[i].getName(), currentKey, context);
			}

			// we consumed the first element , so needs to comunicate to iterator
			grouperIterator.setFirstTupleConsumed(true);
			handler.onGroupElements(grouperIterator, context);

			// This loop consumes the remaining elements that reduce didn't consume
			// The goal of this is to correctly set the last element in the next onCloseGroup() call
			while(iterator.hasNext()) {
				iterator.next();
			}
			
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
	}
  

	/**
	 * Compares sequentially the fields from two tuples and returns which field they differ. 
	 * 
	 * @return
	 */
	private int indexMismatch(ITuple tuple1,ITuple tuple2,int minFieldIndex,int maxFieldIndex){
			for(int i = minFieldIndex; i <= maxFieldIndex; i++) {
				String fieldName = schema.getFields()[i].getName();
				Object object1 = tuple1.getObject(fieldName);
				Object object2 = tuple2.getObject(fieldName);
				if(!tuple1.getObject(fieldName).equals(tuple2.getObject(fieldName))) {
					return i;
				}
			}
			return -1;
	}
}
