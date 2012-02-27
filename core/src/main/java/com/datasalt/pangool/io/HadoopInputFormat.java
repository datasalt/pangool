package com.datasalt.pangool.io;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

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
	
	@Override
  public List getSplits(JobContext context) throws IOException, InterruptedException {
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
