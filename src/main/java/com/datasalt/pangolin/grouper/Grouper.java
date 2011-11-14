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



/**
 * TODO
 * @author epalace
 *
 */
public class Grouper {

	public static final String CONF_SORT_CRITERIA = "grouper.sort.criteria";
	public static final String CONF_FIELDS_GROUP = "grouper.group.fields";
	public static final String CONF_SCHEMA = "grouper.schema";
	
	
	/**
	 * TODO
	 * @author epalace
	 *
	 * @param <KEY_OUT>
	 * @param <VALUE_OUT>
	 */
	public static abstract class Reducer<OUTPUT_KEY,OUTPUT_VALUE> extends org.apache.hadoop.mapreduce.Reducer<Tuple, NullWritable, OUTPUT_KEY,OUTPUT_VALUE> {

	    	//private int currentLevel=0;
	    	private Tuple previousKey=null;
	    	//private Tuple.Prefix currentPrefix = null;
	    	private int maxLevels=0;
	    	
	  

		@Override
		public final void reduce(Tuple key, Iterable<NullWritable> values, Context context) throws IOException {
			Iterator<NullWritable> iterator = values.iterator();
			while (iterator.hasNext()){
				iterator.next();
				//AvroKey<String> avroKey = new AvroKey<String>();
				
				Tuple currentKey = context.getCurrentKey();
				if (previousKey == null){
					for (int i = 0 ; i < maxLevels; i++){
						//onOpenGroup(context,null); //TODO bad
					}
					onElement(currentKey,context);
					previousKey = new Tuple();
					previousKey.set(currentKey);
					
					
				} else {
					int levelMismatch = Tuple.compareLevels(currentKey,previousKey,maxLevels);
					int numClosingGroups = maxLevels -levelMismatch;
					for (int i = 0 ; i < numClosingGroups ; i++){
						//onCloseGroup(context,null); //TODO bad
					}
					for (int i=0 ; i < numClosingGroups; i++){
						//onOpenGroup(context,null);
					}
					onElement(currentKey, context);
					previousKey.set(currentKey);
				}
			}
		}
		
		/**
		 * TODO
		 * @param context
		 * @param prefix
		 */
		protected abstract void onOpenGroup(Context context);

		/**
		 * TODO
		 * @param context
		 * @param prefix
		 */
		protected abstract void onCloseGroup(Context context);

		/**
		 * TODO
		 * @param tuple
		 * @param context
		 */
		protected abstract void onElement(Tuple tuple, Context context);

	}
	
	public void main(String[] args){
		
	}
	
	
	
	public static Schema getSchema(Configuration conf){
		String schemaStr = conf.get(CONF_SCHEMA);
		return Schema.parse(schemaStr);
	}
	

}
