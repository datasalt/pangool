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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author epalace
 *
 * @param <INPUT_KEY>
 * @param <INPUT_VALUE>
 */
public class GrouperMapper<INPUT_KEY,INPUT_VALUE> extends Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>{
	
	private Tuple outputKey = new Tuple();
	private NullWritable outputValue = NullWritable.get();
	private Context context;
	private Schema schema;
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
		try{
		Schema schema = Grouper.getSchema(context.getConfiguration());
		outputKey.setSchema(schema);
		((TupleSortComparator)WritableComparator.get(Tuple.class)).setConf(context.getConfiguration());
		this.context = context;
		} catch(GrouperException e){
			throw new RuntimeException(e);
		}
		
	}
	
	protected Tuple getTupleToEmit(){
		return outputKey;
	}
	
	protected Schema getSchema(){
		return schema;
	}
	
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		
	}
	
	
	@Override
	public void map(INPUT_KEY key, INPUT_VALUE value,Context context) throws IOException,InterruptedException {
		
	}

	protected void emit(Tuple outputKey) throws IOException,InterruptedException {
		context.write(outputKey,outputValue);
	}
	
}
