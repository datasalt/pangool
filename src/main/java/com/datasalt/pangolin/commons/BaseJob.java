package com.datasalt.pangolin.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * <p>Base class for Pangolin jobs. By implementing this class we can run any job 
 * from the command line and from Azkaban. Implement this BaseJob when you want
 * a compatible task with Azkaban, that is injected, and that provides a 
 * {@link Configuration} useful for accessing the HDFS. If what you want
 * is to implement a Hadoop Job, better use {@link BaseHadoopJob}</p>
 * 
 * @author pere
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public abstract class BaseJob {
	
	public abstract double getProgress() throws Exception;
	
	public abstract void cancel() throws Exception;
	
	public abstract void execute(String args[], Configuration conf) throws Exception;
		
	/**
	 * Return Properties to Azkaban
	 * 
	 */
	public abstract Properties getJobGeneratedProperties();

  public static Class<? extends BaseJob> getClass(String className) throws ClassNotFoundException {
		Class cl;
		try 
		{
	    /*
	     * Instantiate Job by reflection
	     */
			cl = Class.forName(className);
		} 
		catch (ClassNotFoundException e) 
		{
			cl = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
		}
		if (!BaseJob.class.isAssignableFrom(cl)) 
		{
			throw new RuntimeException("Class is not of type BaseJob");
		}
		return cl;
	}
	
	

}