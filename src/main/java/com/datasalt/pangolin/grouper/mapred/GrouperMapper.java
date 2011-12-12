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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;


import com.datasalt.pangolin.grouper.GrouperWithRollup;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleImpl;

/**
 * 
 * @author epalace
 *
 * @param <INPUT_KEY>
 * @param <INPUT_VALUE>
 */
public class GrouperMapper<INPUT_KEY,INPUT_VALUE> extends Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>{
	
	//private Tuple outputKey = new Tuple();
//	private NullWritable outputValue = NullWritable.get();
//	private Context context;
//	private FieldsDescription schema;
	private GrouperMapperHandler<INPUT_KEY,INPUT_VALUE> handler;
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
		//try{
		//FieldsDescription schema = FieldsDescription.parse(context.getConfiguration());
		//outputKey.setSchema(schema);
		//registered binary comparator needs to be configured with schema and sort criteria
		//VERY TRICKY!!!
		//((TupleSortComparator)WritableComparator.get(Tuple.class)).setConf(context.getConfiguration());
		//this.context = context;
//		} catch(GrouperException e){
//			throw new RuntimeException(e);
//		}
		
		Configuration conf = context.getConfiguration();
		Class<? extends GrouperMapperHandler> handlerClass = conf.getClass(GrouperWithRollup.CONF_MAPPER_HANDLER,null,GrouperMapperHandler.class); 
		this.handler = ReflectionUtils.newInstance(handlerClass, conf);
		handler.setContext(context);
		handler.setup(context);
	}
	
//	protected Tuple getTupleToEmit(){
//		return outputKey;
//	}
	
//	protected FieldsDescription getSchema(){
//		return schema;
//	}
	
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		handler.cleanup(context);
	}
	
	
	@Override
	public void map(INPUT_KEY key, INPUT_VALUE value,Context context) throws IOException,InterruptedException {
		handler.map(key, value);
	}

//	protected void emit(Tuple outputKey) throws IOException,InterruptedException {
//		context.write(outputKey,outputValue);
//	}
	
}
