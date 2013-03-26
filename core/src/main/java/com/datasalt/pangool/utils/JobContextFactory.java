package com.datasalt.pangool.utils;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

/**
 * A trick that uses reflection to be compatible with both Hadoop 1.0 and Hadoop 2.0
 * In Hadoop 2.0 JobContext is an interface and cannot be instantiated. Instead, one must
 * instantiate JobContextImpl.
 */
public class JobContextFactory {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static JobContext get(Configuration conf, JobID jobId) throws ClassNotFoundException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Class cl = Class.forName(JobContext.class.getName());
		if(cl.isInterface()) {
			// Hadoop 0.23 > , use JobContextImpl
 			cl = Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl");
 		} else {
 			// Hadoop 1.0
 		}
		return (JobContext) cl.getConstructor(Class.forName(Configuration.class.getName()), Class.forName(JobID.class.getName())).newInstance(conf, jobId);
	}
}
