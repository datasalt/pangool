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

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.TupleIterator;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.TupleGroupComparator;
import com.datasalt.pangolin.grouper.io.tuple.TuplePartitioner;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.mapreduce.handler.ReducerHandler;

/**
 * 
 * This {@link Reducer} implements a similar functionality than {@link SimpleReducer} but adding a Rollup feature.
 * 
 * @author eric
 *

 */
public class RollupReducer<OUTPUT_KEY,OUTPUT_VALUE> extends Reducer<ITuple, NullWritable, OUTPUT_KEY,OUTPUT_VALUE> {

	private boolean firstIteration = true;
	private FieldsDescription schema;
	private int minDepth, maxDepth;
	private TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	private ReducerHandler<OUTPUT_KEY, OUTPUT_VALUE> handler;

	protected FieldsDescription getSchema() {
		return schema;
	}
    	
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override  	
  public void setup(Context context) throws IOException,InterruptedException {
  	try{
    Configuration conf = context.getConfiguration();
  	this.schema = FieldsDescription.parse(conf);
  	this.maxDepth = conf.get(TupleGroupComparator.CONF_GROUP_COMPARATOR_FIELDS).split(",").length -1;
  	String confPartitioner = conf.get(TuplePartitioner.CONF_PARTITIONER_FIELDS);
  	this.minDepth = confPartitioner.split(",").length -1;
  	} catch(GrouperException e){
  		throw new RuntimeException(e);
  	}
  	
  	this.grouperIterator = new TupleIterator<OUTPUT_KEY,OUTPUT_VALUE>();
  	this.grouperIterator.setContext(context);
  	
  	Configuration conf = context.getConfiguration();
		Class<? extends ReducerHandler> handlerClass = conf.getClass(Grouper.CONF_REDUCER_HANDLER,null,ReducerHandler.class); 
		this.handler = ReflectionUtils.newInstance(handlerClass, conf);
		handler.setup(schema,context);
  	
  	
  }
  
  public void cleanup(Context context) throws IOException,InterruptedException {
  	handler.cleanup(schema,context);
  }
  

  @Override
  public final void run(Context context) throws IOException,InterruptedException {
  	setup(context);
  	firstIteration=true;
  	while (context.nextKey()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
      //TODO look if this matches super.run() implementation
    }
    
    //close last group
    for (int i=maxDepth; i >=minDepth ; i--){
			handler.onCloseGroup(i,schema.getFields()[i].getName(),context.getCurrentKey(),context);
		}
    cleanup(context);
  }
  
  
  @Override
	public final void reduce(ITuple key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
 	Iterator<NullWritable> iterator = values.iterator();
		grouperIterator.setIterator(iterator);
		iterator.next();
		Tuple currentKey = (Tuple)context.getCurrentKey();
		int indexMismatch;
		if (firstIteration) {
			indexMismatch = minDepth;
			firstIteration=false;
		} else {
			ITuple previousKey = currentKey.getPreviousTuple();
			indexMismatch = indexMismatch(previousKey,currentKey , minDepth,maxDepth);
			for (int i = maxDepth; i >= indexMismatch; i--) {
				handler.onCloseGroup(i, schema.getFields()[i].getName(), previousKey,context);
			}
		}

		for (int i = indexMismatch; i <= maxDepth; i++) {
			handler.onOpenGroup(i, schema.getFields()[i].getName(), currentKey,context);
		}

		// we consumed the first element , so needs to comunicate to iterator
		grouperIterator.setFirstTupleConsumed(true);
		handler.onGroupElements(grouperIterator,context);

		// This loop consumes the remaining elements that reduce didn't consume
		// The goal of this is to correctly set the last element in the next onCloseGroup() call
		while (iterator.hasNext()) {
			iterator.next();
		}
	}
  

	/**
	 * Compares sequentially the fields from two tuples and returns which field they differ. 
	 * 
	 * @return
	 */
	private int indexMismatch(ITuple tuple1,ITuple tuple2,int minFieldIndex,int maxFieldIndex){
		try {
			for(int i = minFieldIndex; i <= maxFieldIndex; i++) {
				String fieldName = schema.getFields()[i].getName();
				if(!tuple1.getObject(fieldName).equals(tuple2.getObject(fieldName))) {
					return i;
				}
			}
			return -1;
		} catch(InvalidFieldException e) {
			throw new RuntimeException(e);
    }
	}
}
