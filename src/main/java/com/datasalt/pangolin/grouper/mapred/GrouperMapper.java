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

import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.io.Tuple;

/**
 * 
 * @author eric
 *

 */
public class GrouperMapper<INPUT_KEY,INPUT_VALUE> extends Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>{
	

	private GrouperMapperHandler<INPUT_KEY,INPUT_VALUE> handler;
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		Class<? extends GrouperMapperHandler> handlerClass = conf.getClass(Grouper.CONF_MAPPER_HANDLER,null,GrouperMapperHandler.class); 
		this.handler = ReflectionUtils.newInstance(handlerClass, conf);
		handler.setContext(context);
		handler.setup(context);
	}
	

	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		handler.cleanup(context);
	}
	
	
	@Override
	public void map(INPUT_KEY key, INPUT_VALUE value,Context context) throws IOException,InterruptedException {
		handler.map(key, value);
	}

}
