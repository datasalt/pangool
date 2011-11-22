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

import com.datasalt.pangolin.grouper.Constants;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.TupleIterator;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.io.Tuple;

/**
 * TODO
 * @author epalace
 *
 * @param <KEY_OUT>
 * @param <VALUE_OUT>
 */
public abstract class GrouperWithRollupReducer<OUTPUT_KEY,OUTPUT_VALUE> extends org.apache.hadoop.mapreduce.Reducer<Tuple, NullWritable, OUTPUT_KEY,OUTPUT_VALUE> {

    	private Tuple lastElementPreviousGroup=null;
    	private Schema schema;
    	private int minDepth,maxDepth;
    	private TupleIterator<OUTPUT_KEY,OUTPUT_VALUE> grouperIterator;
    	private Context context;
    	//private NullWritable outputValue=NullWritable.get();
    	
    	protected Schema getSchema(){
    		return schema;
    	}
    	
    	
  public void setup(Context context) throws IOException,InterruptedException {
  	try{
    Configuration conf = context.getConfiguration();
  	this.schema = Schema.parse(conf);
  	this.minDepth = conf.get(Constants.CONF_MIN_GROUP).split(",").length -1;
  	this.maxDepth = conf.get(Constants.CONF_MAX_GROUP).split(",").length -1;
  	} catch(GrouperException e){
  		throw new RuntimeException(e);
  	}
  	
  	this.grouperIterator = new TupleIterator<OUTPUT_KEY,OUTPUT_VALUE>();
  	this.grouperIterator.setContext(context);
  	this.context = context;
  }

  
  @SuppressWarnings("unchecked")
  @Override
  public final void run(Context context) throws IOException,InterruptedException {

  	setup(context);
  	while (context.nextKey()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
      //TODO look if this matches super.run() implementation
    }
    
    //close last group
    for (int i=maxDepth; i >=minDepth ; i--){
			onCloseGroup(i,schema.getFields()[i].getName(),lastElementPreviousGroup,context);
		}
    cleanup(context);
  }
  
	public final void reduce(Tuple key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		Iterator<NullWritable> iterator = values.iterator();
		grouperIterator.setIterator(iterator);
		iterator.next();

		int indexMismatch;
		if (lastElementPreviousGroup == null) {
			// first iteration
			indexMismatch = minDepth;
			lastElementPreviousGroup = new Tuple();
		} else {
			indexMismatch = indexMismatch(lastElementPreviousGroup, context.getCurrentKey(), minDepth,maxDepth);
			for (int i = maxDepth; i >= indexMismatch; i--) {
				onCloseGroup(i, schema.getFields()[i].getName(), lastElementPreviousGroup, context);
			}
		}

		for (int i = indexMismatch; i <= maxDepth; i++) {
			onOpenGroup(i, schema.getFields()[i].getName(), context.getCurrentKey(),context);
		}

		// we consumed the first element , so needs to comunicate to iterator
		grouperIterator.setFirstTupleConsumed(true);
		onElements(grouperIterator, context);

		// This loop consumes the remaining elements that reduce didn't consume
		// The aim of this is to correctly set the last element in the next onCloseGroup() call
		while (iterator.hasNext()) {
			iterator.next();
		}
		lastElementPreviousGroup.set(context.getCurrentKey());
	}
  

	public abstract void onOpenGroup(int depth,String field,Tuple firstElement,Context context) throws IOException,InterruptedException;
	public abstract void onCloseGroup(int depth,String field,Tuple lastElement,Context context) throws IOException,InterruptedException;
	public abstract void onElements(Iterable<Tuple> tuple, Context context) throws IOException,InterruptedException;
	
	private static int indexMismatch(Tuple tuple1,Tuple tuple2,int minIndex,int maxIndex){
		for (int i=minIndex ; i <=maxIndex ; i++){
			if (!tuple1.get(i).equals(tuple2.get(i))){
				return i;
			}
		}
		return -1;
	}
}
