package com.datasalt.pangool.utils;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * A trick that uses reflection to be compatible with both Hadoop 1.0 and Hadoop 2.0
 * In Hadoop 2.0 TaskAttemptContext is an interface and cannot be instantiated. Instead, one must
 * instantiate TaskAttemptContextImpl.
 */
public class TaskAttemptContextFactory {

	@SuppressWarnings({ "unchecked", "rawtypes" })
  public static TaskAttemptContext get(Configuration conf, TaskAttemptID taskAttemptID) throws ClassNotFoundException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Class cl = Class.forName(TaskAttemptContext.class.getName());
		if(cl.isInterface()) {
			// Hadoop 0.23 > , use TaskAttemptContextImpl
 			cl = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
 		} else {
 			// Hadoop 1.0
 		}
		return (TaskAttemptContext) cl.getConstructor(Class.forName(Configuration.class.getName()), Class.forName(TaskAttemptID.class.getName())).newInstance(conf, taskAttemptID);
	}
}
