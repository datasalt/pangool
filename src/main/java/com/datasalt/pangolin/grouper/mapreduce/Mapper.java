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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;

/**
 * TODO doc
 * @author eric
 *
 */
public abstract class Mapper<INPUT_KEY,INPUT_VALUE> extends org.apache.hadoop.mapreduce.Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>{
	

	//private MapperHandler<INPUT_KEY,INPUT_VALUE> handler;
	private Schema schema;
	private Collector collector;
	//private NullWritable nullValue = NullWritable.get();
	
	
	public static final class Collector {
		private NullWritable nullValue = NullWritable.get();
		private Mapper.Context context;
		Collector(Mapper.Context context){
			this.context = context;
		}
		
		
		public void write(Tuple tuple) throws IOException,InterruptedException {
			context.write(tuple, nullValue);
		}
		
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
	public final void setup(Context context) throws IOException,InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			//Class<? extends MapperHandler> handlerClass = Grouper.getMapperHandler(conf);
			//this.handler = ReflectionUtils.newInstance(handlerClass, conf);
			this.schema = Schema.parse(conf);
			this.collector = new Collector(context);
			setup(schema,context);
			//handler.setup(schema,context);
			
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
	}
	
	public abstract void setup(Schema schema, Context context) throws IOException,InterruptedException;
	

	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		//handler.cleanup(schema,context);
		
	}
	
	
	@Override
	public final void map(INPUT_KEY key, INPUT_VALUE value,Context context) throws IOException,InterruptedException {
		try{
			map(key,value,collector);
		} catch(GrouperException e){
			throw new RuntimeException(e);
		}
	}
	
	public abstract void map(INPUT_KEY key,INPUT_VALUE value,Collector collector) throws IOException,InterruptedException,GrouperException;

//	protected void emit(Tuple tuple,Context context) throws IOException, InterruptedException{
//		context.write(tuple, nullValue);
//	}
	
	
	
}
