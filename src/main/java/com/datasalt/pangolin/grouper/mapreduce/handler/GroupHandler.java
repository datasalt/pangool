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
package com.datasalt.pangolin.grouper.mapreduce.handler;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;

/**
 *
 * This is the common interface that any {@link Grouper} job needs to implement. This handler is called in the reducer step by
 * {@link SimpleReducer} or {@link RollupReducer} depennding if Roll-up feature is used.
 * 
 * @author eric
 *
 */
@SuppressWarnings("rawtypes")
public abstract class GroupHandler<OUTPUT_KEY,OUTPUT_VALUE> {
	
	
	/**
	 * 
	 * @param schema The schema from the tuples
	 * @param context See {@link Reducer.Context}
	 */
	public void setup(Schema schema, Reducer.Context context) throws IOException,InterruptedException,GrouperException {
		
	}
	
	public void cleanup(Schema schema, Reducer.Context context) throws IOException,InterruptedException,GrouperException {
		
	}
	
	
	/**
	 * 
	 * This is the method called any time that a sub-group is opened when rollup is used. 
	 * Check {@link Grouper} doc about how roll-up feature works
	 * 
	 * @param depth The tuple's field index that is currently being opened.0 when it's the first field
	 * @param field The tuple's field name that is currently being opened.
	 * @param firstElement The first tuple from the current group
	 * @param context The reducer context as in {@link Reducer} 
	 * 
	 */
	public abstract void onOpenGroup(int depth,String field,ITuple firstElement, Reducer.Context context) throws IOException,InterruptedException,GrouperException ;
	
	
	
	/**
	 * 
	 * This is the method called after every sub-group is being closed when rollup is used. 
	 * Check {@link Grouper} doc about how roll-up feature works
	 * 
	 * @param depth The tuple's field index that is currently being opened.It's 0 when it's the first field
	 * @param field The tuple's field name that is currently being opened.
	 * @param firstElement The last tuple from the current group
	 * @param context The reducer context as in {@link Reducer} 
	 * 
	 */
	public abstract void onCloseGroup(int depth,String field,ITuple lastElement,Context context) throws IOException,InterruptedException,GrouperException;
	
	
	
	/**
	 * 
	 * This is method is called with an iterable that contains all the tuples that have been grouped by 
	 * the fields defined in {@link Grouper#setFieldsToGroupBy(String...)}
	 * 
	 * @param tuples Iterable that contains all the tuples from a group
	 * @param context The reducer context as in {@link Reducer}
	 */
	public abstract void onGroupElements(Iterable<ITuple> tuples,Context context) throws IOException,InterruptedException,GrouperException ;
	
}
