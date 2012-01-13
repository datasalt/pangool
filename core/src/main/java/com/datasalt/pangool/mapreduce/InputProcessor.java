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

package com.datasalt.pangool.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;

/**
 * TODO doc
 */
@SuppressWarnings("rawtypes") 
public abstract class InputProcessor<INPUT_KEY,INPUT_VALUE> extends Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>{
	
	private Collector collector;
	
	public static final class Collector {
		private NullWritable nullValue = NullWritable.get();
    private Mapper.Context context;
    private Schema schema;
    
		Collector(Schema schema, Mapper.Context context){
			this.schema = schema;
			this.context = context;
		}
		
		/**
		 * Return the Hadoop {@link Mapper.Context}.  
		 */
		public Mapper.Context getHadoopContext() {
			return context;
		}
		
		@SuppressWarnings("unchecked")
    public void write(ITuple tuple) throws IOException,InterruptedException {
			context.write(tuple, nullValue);
		}		
	}
	
	/**
	 * Do not override. Override {@link InputProcessor#setup(Collector)} instead.
	 */
  @Override
	public final void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			Schema schema = Schema.parse(conf);
			this.collector = new Collector(schema, context);
			setup(collector);
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
	}

  /**
   * Called once at the start of the task. Override it to implement
	 * your custom logic. 
   */
	public void setup(Collector collector) throws IOException,InterruptedException,GrouperException {
		
	}	

	/**
	 * Do not override. Override {@link InputProcessor#cleanup(Collector)} instead.
	 */
	@Override
	public final void cleanup(Context context) throws IOException,InterruptedException {
		cleanup(collector);
	}
	
	/**
	 * Called once at the end of the task. Override it to implement
	 * your custom logic. 
	 */
	public void cleanup(Collector collector) throws IOException,InterruptedException {
	}
	
	/**
	 * Do not override! Override {@link InputProcessor#process(Object, Object, Collector)} instead.
	 */
	@Override
	public final void map(INPUT_KEY key, INPUT_VALUE value, Context context) throws IOException,InterruptedException {
		try{
			process(key,value,collector);
		} catch(GrouperException e){
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Called once per each input pair of key/values. Override it to implement
	 * your custom logic. 
	 */
	public abstract void process(INPUT_KEY key,INPUT_VALUE value,Collector collector) throws IOException,InterruptedException,GrouperException;
}
