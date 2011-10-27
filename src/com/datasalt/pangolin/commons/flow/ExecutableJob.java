package com.datasalt.pangolin.commons.flow;


import org.apache.hadoop.conf.Configuration;

import com.datasalt.pangolin.commons.BaseHadoopJob;



public class ExecutableJob<T extends BaseHadoopJob> implements Executable<Configuration>{

	Class<T> jobClass;
	String[] args;
	
	public ExecutableJob(Class<T> job, String[] args) {
		this.jobClass = job;
		this.args = args;
	}
	
	@Override
  public void execute(Configuration conf) throws Exception {
		T job = jobClass.newInstance();
		job.getJob(args, conf).waitForCompletion(true);
  }
}
