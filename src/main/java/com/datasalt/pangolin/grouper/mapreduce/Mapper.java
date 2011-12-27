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
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.mapreduce.handler.MapperHandler;

/**
 * TODO doc
 * @author eric
 *
 */
public class Mapper<INPUT_KEY,INPUT_VALUE> extends org.apache.hadoop.mapreduce.Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>{
	

	private MapperHandler<INPUT_KEY,INPUT_VALUE> handler;
	private FieldsDescription schema;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
	public void setup(Context context) throws IOException,InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			Class<? extends MapperHandler> handlerClass = conf.getClass(Grouper.CONF_INPUT_HANDLER, null, MapperHandler.class);
			this.handler = ReflectionUtils.newInstance(handlerClass, conf);

			this.schema = FieldsDescription.parse(conf);
			//Tuple outputTuple = TupleFactory.createTuple(schema);
			handler.setup(schema,context);
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
	}
	

	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		handler.cleanup(schema,context);
	}
	
	
	@Override
	public void map(INPUT_KEY key, INPUT_VALUE value,Context context) throws IOException,InterruptedException {
		handler.map(key, value,context);
	}

}
