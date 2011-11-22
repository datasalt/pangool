package com.datasalt.pangolin.commons.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Before;

import com.datasalt.pangolin.commons.io.ProtoStuffSerialization;
import com.datasalt.pangolin.io.Serialization;



public abstract class AbstractBaseTest {

	private static Logger log = Logger.getLogger(AbstractBaseTest.class);
	
	private Configuration conf;
	protected Serialization ser; 

	public Serialization getSer() throws IOException {
		if (ser == null) {
			ser = new Serialization(getConf());	
		}
		return ser;
	}

	public Configuration getConf() throws IOException {
		if (conf == null){
			conf =createConf();
		}
		return conf;
	}

	public static Configuration createNewConfiguration() {
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");		
		org.apache.hadoop.mapred.JobConf conf = new org.apache.hadoop.mapred.JobConf();
		
		for(String property : new String[] { "mapred.child.java.opts", "mapred.map.child.java.opts", "mapred.reduce.child.java.opts" }) {
			String mapredChildJavaOpts = conf.get(property, "") + " " +
			 "-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl";
			conf.set(property, mapredChildJavaOpts.trim());
			log.info("Setting [" + property + "] to [" + mapredChildJavaOpts.trim() + "]");
		}
		return conf;
	}
	
	private static void configureSerialization(Configuration conf) {
		// Adding the Thrift serialization
		String ser = conf.get("io.serializations").trim();
		if (ser.length() !=0 ) {
			ser += ",";
		}
		ser += "org.apache.hadoop.contrib.serialization.thrift.ThriftSerialization";
		ser += "," + ProtoStuffSerialization.class.getName();
		conf.set("io.serializations", ser);
	}
	
	
	private Configuration createConf(){
		Configuration conf = createNewConfiguration();
		configureSerialization(conf);
		return conf;
	}
	

	@Before
	public void prepare() throws IOException {

	}
	
}
