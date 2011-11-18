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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

/**
 * TODO
 * @author epalace
 *
 * @param <KEY_OUT>
 * @param <VALUE_OUT>
 */
public class GrouperReducer<OUTPUT_KEY,OUTPUT_VALUE> extends org.apache.hadoop.mapreduce.Reducer<Tuple, NullWritable, OUTPUT_KEY,OUTPUT_VALUE> {

    	private Tuple previousKey=null;
    	private Schema schema;
    	private int minDepth,maxDepth;
    	private GrouperIterator<OUTPUT_KEY,OUTPUT_VALUE> grouperIterator;
    	
  public void setup(Context context) throws IOException,InterruptedException {
  	try{
    Configuration conf = context.getConfiguration();
  	this.schema = Grouper.getSchema(conf);
  	this.minDepth = conf.get(Grouper.CONF_MIN_GROUP).split(",").length -1;
  	this.maxDepth = conf.get(Grouper.CONF_MAX_GROUP).split(",").length -1;
  	} catch(GrouperException e){
  		throw new RuntimeException(e);
  	}
  	
  	this.grouperIterator = new GrouperIterator<OUTPUT_KEY,OUTPUT_VALUE>();
  	this.grouperIterator.setContext(context);
  }

  
  @SuppressWarnings("unchecked")
  @Override
  public final void run(Context context) throws IOException,InterruptedException {
  	//super.run(context);
  	
  	setup(context);
    while (context.nextKey()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
      // If a back up store is used, reset it
//      ((ReduceContext.ValueIterator)
//          (context.getValues().iterator())).resetBackupStore();
    }
    
    //close last group
    for (int i=maxDepth; i >=minDepth ; i--){
			onCloseGroup(i,schema.getFields()[i].getName(),context.getCurrentKey(),context);
		}
    
    
    cleanup(context);
  }
  
	public final void reduce(Tuple key, Iterable<NullWritable> values, Context context) throws IOException,
	    InterruptedException {
		Iterator<NullWritable> iterator = values.iterator();

		// boolean firstIteration = true;
		while(iterator.hasNext()) {
			grouperIterator.setIterator(iterator);
			iterator.next();
			Tuple currentKey = context.getCurrentKey();
			int indexMismatch;
			if(previousKey == null) {
				// FIRST ITERATION
				indexMismatch = minDepth;
				previousKey = new Tuple();
			} else {
				indexMismatch = indexMismatch(previousKey, currentKey, minDepth, maxDepth);
				//System.out.println("Index mismatch : " + previousKey + " , " + currentKey + " => " + indexMismatch);
				for(int i = maxDepth; i >= indexMismatch; i--) {
					onCloseGroup(i,schema.getFields()[i].getName(), previousKey, context);
				}
			}

			for(int i = indexMismatch; i <= maxDepth; i++) {
				onOpenGroup(i,schema.getFields()[i].getName(),currentKey, context);
			}
			grouperIterator.setFirstAvailable(true);
			onElements(grouperIterator, context);
			previousKey.set(currentKey);
		}
	}
  
  
  
	//@Override
//	public final void reduce2(Tuple key, Iterable<NullWritable> values, Context context) throws IOException {
//		
//		Iterator<NullWritable> iterator = values.iterator();
//		while (iterator.hasNext()){
//			
//			iterator.next();
//			
//			Tuple currentKey = context.getCurrentKey();
//			if (previousKey == null){
//				for (int i = 0 ; i < maxLevels; i++){
//					//onOpenGroup(context,null); //TODO bad
//				}
//				//onElement(currentKey,context);
//				previousKey = new Tuple();
//				previousKey.set(currentKey);
//				
//				
//			} else {
//				int levelMismatch = Tuple.compareLevels(currentKey,previousKey,maxLevels);
//				int numClosingGroups = maxLevels -levelMismatch;
//				for (int i = 0 ; i < numClosingGroups ; i++){
//					//onCloseGroup(context,null); //TODO bad
//				}
//				for (int i=0 ; i < numClosingGroups; i++){
//					//onOpenGroup(context,null);
//				}
//				//onElement(currentKey, context);
//				previousKey.set(currentKey);
//			}
//		}
//	}
	
	
	
	
	/**
	 * TODO
	 * @param context
	 * @param prefix
	 */
	public void onOpenGroup(int depth,String field,Tuple firstElement,Context context) throws IOException,InterruptedException {
		
	}

	/**
	 * TODO
	 * @param context
	 * @param prefix
	 */
	public void onCloseGroup(int depth,String field,Tuple lastElement,Context context) throws IOException,InterruptedException {
		
	}

	

	public void onElements(Iterator<Tuple> tuple, Context context) throws IOException,InterruptedException {
		
	  
  }
	
	public static int indexMismatch(Tuple tuple1,Tuple tuple2,int minIndex,int maxIndex){
		for (int i=minIndex ; i <=maxIndex ; i++){
			if (!tuple1.get(i).equals(tuple2.get(i))){
				return i;
			}
		}
		return -1;
	}
	
	
	public static boolean partialEquals(Tuple tuple1,Tuple tuple2, int minIndex,int maxIndex){
		for (int i=minIndex ; i <=maxIndex ; i++){
			if (!tuple1.get(i).equals(tuple2.get(i))){
				return false;
			}
		}
		return true;
	}

}
