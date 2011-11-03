package com.datasalt.pangolin.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class CommonUtils {

	@SuppressWarnings({ "rawtypes", "unchecked" })
  public static Map invertMap(Map<?,?> map){
		Map result = new TreeMap();
		for (Map.Entry entry : map.entrySet()){
  		result.put(entry.getValue(),entry.getKey());
  	}
		return result;
	}
	
	/**
	 * Executes whichever BaseJob. Canonical Class nam comming as first parameter . 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * Parse arguments like -D mapred. ... = ...
		 */
		
		PangolinConfigurationFactory factory = PangolinConfigurationFactory.getInstance();
		Configuration conf = factory.getConf();
		
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
	  String[] arguments = parser.getRemainingArgs();
	  BaseJob job = BaseJob.getClass(arguments[0]).newInstance();
		
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
		main(largs.toArray(new String[0]));
	}
	
	
}
