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

package org.apache.avro.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.lib.output.AvroOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/** An {@link org.apache.hadoop.mapred.InputFormat} for Avro data files */
public class AvroInputFormat<T>
  extends FileInputFormat<AvroWrapper<T>, NullWritable> {

	@Override
	public RecordReader<AvroWrapper<T>, NullWritable> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context) throws IOException,
			InterruptedException {
		context.setStatus(inputSplit.toString());
		return new AvroRecordReader<T>(context.getConfiguration(), (FileSplit)inputSplit);
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