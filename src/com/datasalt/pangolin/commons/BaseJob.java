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
	
	/**
	 * Executes whichever BaseJob. Canonical Class nam comming as first parameter . 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new PangolinGuiceModule());

		/*
		 * Crate a Hadoop Configuration object with our own configuration
		 */
		Configuration conf = injector.getInstance(PangolinConfigurationFactory.class).create();
		/*
		 * Parse arguments like -D mapred. ... = ...
		 */
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
	  String[] arguments = parser.getRemainingArgs();
		BaseJob job = injector.getInstance(getClass(arguments[0]));
    /*
     * Run job
     */
		//TODO Add log of execution start.
		job.execute(new ArrayList<String>(Arrays.asList(arguments)).subList(1, arguments.length).toArray(new String[0]), conf);
	}

	/**
	 * Main to be called by each individual Job main, just a wrapper 
	 * to the regular main that provides the class name.
	 * @throws Exception 
	 */
	public static void main(Class<? extends BaseJob> jobClass, String args[]) throws Exception {
		ArrayList<String> largs = new ArrayList<String>(Arrays.asList(args));
		largs.add(0, jobClass.getCanonicalName());
		BaseJob.main(largs.toArray(new String[0]));
	}

}