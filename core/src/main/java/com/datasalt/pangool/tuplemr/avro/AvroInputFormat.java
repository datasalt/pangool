/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasalt.pangool.tuplemr.avro;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.lib.output.AvroOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/** 
 * This is a Pangool's implementation of {@link org.apache.avro.mapred.AvroInputFormat}.
 * Instead of being configured via {@link Configuration}, its state is defined via 
 * instantiation using Java-serialization.    
 */
@SuppressWarnings("serial")
public class AvroInputFormat<T>
  extends FileInputFormat<AvroWrapper<T>, NullWritable> implements Serializable{

	private transient Schema schema;
	private String schemaStr;
	private boolean isReflect;
	
	public AvroInputFormat(Schema schema){
		this.schema = schema;
		this.schemaStr = schema.toString();
	}
	
	public AvroInputFormat(Schema schema,boolean isReflect){
		this(schema);
		this.isReflect = isReflect;
	}
	
	private Schema getSchema(){
		if (schema == null){
			schema = new Schema.Parser().parse(schemaStr);
		}
		return schema;
	}
	
	@Override
	public RecordReader<AvroWrapper<T>, NullWritable> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context) throws IOException,
			InterruptedException {
		context.setStatus(inputSplit.toString());
		return new AvroRecordReader<T>(getSchema(),isReflect,
				context.getConfiguration(), (FileSplit)inputSplit);
	}

	@Override
	protected List<FileStatus> listStatus(JobContext job
      ) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();
    for (FileStatus file : super.listStatus(job)){
    	String fileName = file.getPath().getName();
      if (fileName.endsWith(AvroOutputFormat.EXT)){
      	result.add(file);
      }
    }
    return result;
	}
}