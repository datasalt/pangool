package com.datasalt.pangool.tuplemr.mapred.lib.output;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

public class HadoopOutputFormat extends OutputFormat implements Configurable, Serializable {

	private Class<? extends OutputFormat>  clazz;
	private OutputFormat instance;
	private Configuration conf;
	
	public HadoopOutputFormat(Class<? extends OutputFormat> wrappedOutputFormat) {
		this.clazz = wrappedOutputFormat;
	}
	
	private void instantiateWhenNeeded() {
		if(instance == null) {
			instance = ReflectionUtils.newInstance(clazz, conf);
		}
	}
	@Override
  public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		instantiateWhenNeeded();
	  return instance.getRecordWriter(context);
  }

	@Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		instantiateWhenNeeded();
		instance.checkOutputSpecs(context);
	}

	@Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		instantiateWhenNeeded();
		return instance.getOutputCommitter(context);
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
