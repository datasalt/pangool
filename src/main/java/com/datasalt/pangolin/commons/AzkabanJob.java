package com.datasalt.pangolin.commons;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import azkaban.common.utils.Props;

/**
 * <p>This class can be called from Azkaban to run any Job that implements BaseJob.</p>
 * <p>It implements its API as described <a href='http://sna-projects.com/azkaban/documentation.php'>here</a>.</p>
 * <ul>
 * <li>The job class name is configured in Azkaban under "pangolin.job.class.name"</li>
 * <li>The hadoop args(-Dmapred...) are configured under "pangolin.job.hadoop.args"</li>
 * <li>The job args are configured under "pangolin.job.args"</li>
 * </ul>
 * <p>All the properties passed by Azkaban will be automatically added to the Hadoop Configuration so that 
 * they are inmediately accessible from BaseJob implementations - both in the master JVM and the Task JVM's.</p>
 *  
 * @author pere
 *
 */
public class AzkabanJob {

	static Logger log = LoggerFactory.getLogger(AzkabanJob.class);
	
	public final static String JOB_CLASS_NAME  = "pangolin.job.class.name";
	public final static String JOB_CLASS_ARGS  = "pangolin.job.args";
	public final static String JOB_HADOOP_ARGS = "pangolin.job.hadoop.args";
	
	Properties props;
	String name;
	BaseJob job;
	


	
	private PangolinConfigurationFactory factory = PangolinConfigurationFactory.getInstance();
	
	/**
	 * Azkaban calls this constructor with the name and the configuration
	 * 
	 * @param name The name of the job as provided by Azkaban
	 * @param props The properties that come from Azkaban
	 */
	public AzkabanJob(String name, Properties props) {
		this.name = name;
		this.props = props;
		
		
	}
	
	/**
	 * Run the BaseJob according to what we have in the configuration that comes from Azkaban
	 * 
	 * @throws Exception
	 */
	public void run() throws Exception {
		String className = (String) props.get(JOB_CLASS_NAME);
		String[] args = { }, hadoopArgs = { };
		String sArgs = props.getProperty(JOB_CLASS_ARGS);
		if(sArgs != null) {
			args = sArgs.split(" ");
		}
		String hArgs = props.getProperty(JOB_HADOOP_ARGS);
		
		log.info("Job class name: " + className);
		log.info("Arguments: " + sArgs);
		log.info("Hadoop Arguments: " + hArgs);
		
		if(hArgs != null) {
			hadoopArgs = hArgs.split(" ");
		}
		LinkedList<String> argList = new LinkedList<String>(Arrays.asList(hadoopArgs));
		// Pass Azkaban properties as Hadoop properties so they are added to the Configuration
		for(Map.Entry<Object, Object> entry: props.entrySet()) {
			argList.add("-D " + entry.getKey() + "=" + entry.getValue());
		}
		// Create configuration
		Configuration conf = factory.getConf();
		// Adding the properties comming from azkaban to the config
		fillConfig(conf, props);
		// Parse Hadoop arguments
		GenericOptionsParser parser = new GenericOptionsParser(conf, argList.toArray(new String[0]));
		parser.getRemainingArgs();
		// Get BaseJob instance by reflection
		job = BaseJob.getClass(className).newInstance();
		// Get the Hadoop job
		job.execute(args, conf);
	}
	
	
	private static void fillConfig(Configuration conf, Properties prop) {
			for (Object okey : prop.keySet()) {
				String key = (String) okey;
				conf.set(key, prop.getProperty(key));
			}
	}
	
	/**
	 * Get the progress. 
	 * Right now this is not working as Azkaban doesn't implement progress polling from Java processes.
	 */
	public double getProgress() throws Exception {
		return job.getProgress();
	}
	
	/**
	 * Return Properties to Azkaban 
	 */
	public Props getJobGeneratedProperties() {
		Properties props = job.getJobGeneratedProperties();
		return new Props(new Props(), props);
	}
	
	/**
	 * Called when the job has been cancelled.
	 * 
	 * @throws Exception
	 */
	public void cancel() throws Exception {
		job.cancel();
	}
}
