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
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.TupleIterator;
import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.mapreduce.handler.ReducerHandler;

/**
 * TODO
 * @author eric
 *
 */
public class SimpleReducer<OUTPUT_KEY,OUTPUT_VALUE> extends Reducer<ITuple, NullWritable, OUTPUT_KEY,OUTPUT_VALUE> {

	private FieldsDescription schema;
	private TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	private ReducerHandler<OUTPUT_KEY, OUTPUT_VALUE> handler;

  @SuppressWarnings({"unchecked","rawtypes"})
  public void setup(Context context) throws IOException,InterruptedException {
  	//System.out.println("SimpleReducer setup");
		super.setup(context);
		try {
			Configuration conf = context.getConfiguration();
			this.schema = FieldsDescription.parse(conf);
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
  	
  	this.grouperIterator = new TupleIterator<OUTPUT_KEY,OUTPUT_VALUE>();
  	this.grouperIterator.setContext(context);
  	
  	Configuration conf = context.getConfiguration();
    Class<? extends ReducerHandler> handlerClass = Grouper.getReducerHandler(conf);
    
		this.handler = ReflectionUtils.newInstance(handlerClass, conf);
		
		handler.setup(schema,context);
  	
  }
  @Override
  public void cleanup(Context context) throws IOException,InterruptedException {
  	super.cleanup(context);
  	handler.cleanup(schema,context);
  }
  

  @Override
  public final void run(Context context) throws IOException,InterruptedException {
  	super.run(context);
  }
  
  @Override
	public final void reduce(ITuple key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		Iterator<NullWritable> iterator = values.iterator();
		grouperIterator.setIterator(iterator);
		handler.onGroupElements(grouperIterator,context);
		
	}
  
}
