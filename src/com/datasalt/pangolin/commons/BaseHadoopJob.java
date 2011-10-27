package com.datasalt.pangolin.commons;

import java.io.IOException;
import java.util.Properties;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * A BaseJob ready for deal with Hadoop Jobs. Override the getJob()
 * method to supply your own Hadoop Job.
 * <br/>
 * Override getJobGeneratedProperties() to return back properties to
 * Azkaban
 * <br/>
 * 
 * @author ivan
 */
public abstract class BaseHadoopJob extends BaseJob {

	boolean submitted = false;
	Job theJob;
	
	/**
	 * This method receives the Job's parameters just as they are passed from the command line. The Hadoop Configuration is provided externally depending on the use case. 
	 * 
	 * @param args The command-line-style arguments needed to instantiate this Job 
	 * @param conf The provided Configuration
	 * @return The Hadoop Job configured according to the arguments and the provided Configuration 
	 * 
	 * @throws IOException
	 */
	public abstract Job getJob(String[] args, Configuration conf) throws IOException;
	
	/**
	 * This method can be used to return properties from this job (e.g. counters, statistics, generated properties...) 
	 * to Azkaban
	 * 
	 * @return
	 */
	public abstract Properties getJobGeneratedProperties(Job job);

	@Override
  public Properties getJobGeneratedProperties() {
	  return getJobGeneratedProperties(theJob);
  }

	/**
	 * Get the progress: [0, 0.5] for map phase, [0.5, 1.0] for reduce phase. Right now this is not working as Azkaban doesn't implement progress polling from Java processes.
	 * 
	 * @return The progress of the Job based on Map / Reduce phases progress
	 * @throws Exception
	 */
	@Override
  public double getProgress() throws Exception {
		/*
		 * We can't query for the Job progress meanwhile it is being defined
		 */
		if(!submitted) {
			return 0.0;
		}
		if(theJob.reduceProgress() == 0.0) {
	    return theJob.mapProgress() * 0.5;
		}
		return theJob.reduceProgress() * 0.5 + 0.5;
  }

	/**
	 * Cancel this Job: it will also cancel the Hadoop Job by killing it in the Job Tracker.
	 * 
	 */
	@Override
  public void cancel() throws Exception {
		theJob.killJob();
	}

	@Override
  public void execute(String[] args, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		theJob = getJob(args, conf);
		
		theJob.submit();
		submitted = true;
		boolean success = theJob.waitForCompletion(true);
		if (!success) {
			throw new IOException("JOB FAILED!");
		}
  }
}
