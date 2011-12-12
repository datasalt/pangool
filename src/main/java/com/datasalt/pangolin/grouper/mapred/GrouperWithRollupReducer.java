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

package com.datasalt.pangolin.grouper.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.Constants;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.GrouperWithRollup;
import com.datasalt.pangolin.grouper.TupleIterator;
import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.io.DoubleBufferedTuple;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleImpl;
import com.datasalt.pangolin.grouper.io.TupleImpl.InvalidFieldException;

/**
 * TODO
 * @author epalace
 *
 * @param <KEY_OUT>
 * @param <VALUE_OUT>
 */
public class GrouperWithRollupReducer<OUTPUT_KEY,OUTPUT_VALUE> extends org.apache.hadoop.mapreduce.Reducer<DoubleBufferedTuple, NullWritable, OUTPUT_KEY,OUTPUT_VALUE> {

    	//private DoubleBufferedTuple lastElementPreviousGroup=null;
		 	private boolean firstIteration=true;
    	private FieldsDescription schema;
    	private int minDepth,maxDepth;
    	private TupleIterator<OUTPUT_KEY,OUTPUT_VALUE> grouperIterator;
    	private GrouperReducerHandler<OUTPUT_KEY,OUTPUT_VALUE> handler;
    	
    	protected FieldsDescription getSchema(){
    		return schema;
    	}
    	
  @Override  	
  public void setup(Context context) throws IOException,InterruptedException {
  	try{
    Configuration conf = context.getConfiguration();
  	this.schema = FieldsDescription.parse(conf);
  	this.minDepth = conf.get(Constants.CONF_MIN_GROUP).split(",").length -1;
  	this.maxDepth = conf.get(Constants.CONF_MAX_GROUP).split(",").length -1;
  	} catch(GrouperException e){
  		throw new RuntimeException(e);
  	}
  	
  	this.grouperIterator = new TupleIterator<OUTPUT_KEY,OUTPUT_VALUE>();
  	this.grouperIterator.setContext(context);
  	
  	Configuration conf = context.getConfiguration();
		Class<? extends GrouperReducerHandler> handlerClass = conf.getClass(GrouperWithRollup.CONF_REDUCER_HANDLER,null,GrouperReducerHandler.class); 
		this.handler = ReflectionUtils.newInstance(handlerClass, conf);
		handler.setup(context);
  	
  	
  }
  
  public void cleanup(Context context) throws IOException,InterruptedException {
  	handler.cleanup(context);
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
			handler.onCloseGroup(i,schema.getFields()[i].getName(),context.getCurrentKey());
		}
    cleanup(context);
  }
  
  
  @Override
	public final void reduce(DoubleBufferedTuple key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		Iterator<NullWritable> iterator = values.iterator();
		grouperIterator.setIterator(iterator);
		iterator.next();
		DoubleBufferedTuple currentKey = context.getCurrentKey();
		Tuple previousKey = currentKey.getPreviousTuple();
		int indexMismatch;
		if (firstIteration) {
			// first iteration
			indexMismatch = minDepth;
			firstIteration=false;
		} else {
			indexMismatch = indexMismatch(previousKey,currentKey , minDepth,maxDepth);
			for (int i = maxDepth; i >= indexMismatch; i--) {
				handler.onCloseGroup(i, schema.getFields()[i].getName(), previousKey);
			}
		}

		for (int i = indexMismatch; i <= maxDepth; i++) {
			handler.onOpenGroup(i, schema.getFields()[i].getName(), currentKey);
		}

		// we consumed the first element , so needs to comunicate to iterator
		grouperIterator.setFirstTupleConsumed(true);
		handler.onGroupElements(grouperIterator);

		// This loop consumes the remaining elements that reduce didn't consume
		// The aim of this is to correctly set the last element in the next onCloseGroup() call
		while (iterator.hasNext()) {
			iterator.next();
		}
		//lastElementPreviousGroup.deepCopyFrom(context.getCurrentKey());
	}
  

	
	private int indexMismatch(Tuple tuple1,Tuple tuple2,int minIndex,int maxIndex){
		try {
			for(int i = minIndex; i <= maxIndex; i++) {
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
