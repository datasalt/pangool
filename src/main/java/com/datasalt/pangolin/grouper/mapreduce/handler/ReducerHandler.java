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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;

public abstract class ReducerHandler<OUTPUT_KEY,OUTPUT_VALUE> {
	
	private Reducer<ITuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE>.Context context;
	
	public void setup(FieldsDescription schema,Reducer<ITuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE>.Context context) throws IOException,InterruptedException {
		
	}
	
	public void cleanup(FieldsDescription schema,Reducer<ITuple,NullWritable,OUTPUT_KEY,OUTPUT_VALUE>.Context context) throws IOException,InterruptedException {
		
	}
	
	public abstract void onOpenGroup(int depth,String field,ITuple firstElement,Context context) throws IOException,InterruptedException;
	
	
	public abstract void onCloseGroup(int depth,String field,ITuple lastElement,Context context) throws IOException,InterruptedException;
	
	
	public abstract void onGroupElements(Iterable<ITuple> tuples,Context context) throws IOException,InterruptedException;
	
	
//	protected void emit(OUTPUT_KEY outputKey,OUTPUT_VALUE outputValue) throws IOException, InterruptedException{
//		context.write(outputKey, outputValue);
//	}
	
}
