package com.datasalt.pangolin.commons.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Before;

import com.datasalt.pangolin.commons.io.ProtoStuffSerialization;
import com.datasalt.pangolin.grouper.io.tuple.serialization.TupleSerialization;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.serialization.thrift.ThriftSerialization;



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
		ThriftSerialization.enableThriftSerialization(conf);
		ProtoStuffSerialization.enableProtoStuffSerialization(conf);
		TupleSerialization.enableTupleSerialization(conf);
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
