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
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;

/**
 * TODO doc
 * @author eric
 *
 * 
 */
public abstract class MapperHandler<INPUT_KEY,INPUT_VALUE> {
	private NullWritable nullValue = NullWritable.get();
	
	
	public void setup(FieldsDescription schema,Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>.Context context) throws IOException,InterruptedException {
		
	}
	
	public void cleanup(FieldsDescription schema,Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>.Context context) throws IOException,InterruptedException {
		
	}
	
	public void map(INPUT_KEY key,INPUT_VALUE value,Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>.Context context) throws IOException, InterruptedException{
		
	}
	
	protected void emit(Tuple tuple,Mapper<INPUT_KEY,INPUT_VALUE,Tuple,NullWritable>.Context context) throws IOException, InterruptedException{
		context.write(tuple, nullValue);
	}
	
}
