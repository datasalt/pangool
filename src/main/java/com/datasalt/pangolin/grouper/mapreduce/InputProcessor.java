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
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;

/**
 * TODO doc
 * @author eric
 *
 */
public abstract class InputProcessor<INPUT_KEY,INPUT_VALUE> extends Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>{
	
	private Schema schema;
	private Collector collector;
	
	public static final class Collector {
		private NullWritable nullValue = NullWritable.get();
		private Mapper.Context context;
		Collector(Mapper.Context context){
			this.context = context;
		}
		
		
		public void write(Tuple tuple) throws IOException,InterruptedException {
			context.write(tuple, nullValue);
		}
		
		public Mapper.Context getContext() {
			return context;
		}
		
	}
	
	
  @Override
	public final void setup(Context context) throws IOException,InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			this.schema = Schema.parse(conf);
			this.collector = new Collector(context);
			setup(schema,context);
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
	}
	
	//TODO should this be blank , not abstract ?
	public abstract void setup(Schema schema, Context context) throws IOException,InterruptedException,GrouperException ;
	

	@Override
	//TODO should we delegate this one as well . Another clenaup method ??
	public void cleanup(Context context) throws IOException,InterruptedException {
	}
	
	
	@Override
	public final void map(INPUT_KEY key, INPUT_VALUE value,Context context) throws IOException,InterruptedException {
		try{
			process(key,value,collector);
		} catch(GrouperException e){
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * This is the actual method that needs to be implemented by mappers used in {@link Grouper}
	 * @param key
	 * @param value
	 * @param collector
	 * 
	 */
	public abstract void process(INPUT_KEY key,INPUT_VALUE value,Collector collector) throws IOException,InterruptedException,GrouperException;
	
}
