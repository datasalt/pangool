/**
 * Copyright [2012] [Datasalt Systems S.L.]
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
package com.datasalt.pangool.tuplemr.mapred.lib.input;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings({ "serial", "rawtypes" })
public class HadoopInputFormat extends InputFormat implements Serializable, Configurable {

	private Class<? extends InputFormat> clazz;
	private Configuration conf;
	private InputFormat instance;
	
	public HadoopInputFormat(Class<? extends InputFormat> clazz) {
	  this.clazz = clazz;
  }

	private void instantiateWhenNeeded() {
		if(instance == null) {
			instance = ReflectionUtils.newInstance(clazz, conf);
		}
	}
	
	@SuppressWarnings("unchecked")
  @Override
  public List<InputSplit> getSplits(JobContext context) 
  throws IOException, InterruptedException {
		instantiateWhenNeeded();
		return instance.getSplits(context);
  }

	@Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
		instantiateWhenNeeded();
		return instance.createRecordReader(split, context);
	}

	@Override
  public void setConf(Configuration conf) {
	  this.conf = conf;
  }

	@Override
  public Configuration getConf() {
		return conf;
	}

}
