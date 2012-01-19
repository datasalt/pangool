package com.datasalt.pangolin.commons.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;

import com.datasalt.pangolin.commons.io.ProtoStuffSerialization;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.serialization.thrift.ThriftSerialization;
import com.datasalt.pangool.io.tuple.ser.TupleInternalSerialization;



public abstract class AbstractBaseTest {
	
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
		org.apache.hadoop.mapred.JobConf conf = new org.apache.hadoop.mapred.JobConf();
		return conf;
	}
	
	private static void configureSerialization(Configuration conf) {
		ThriftSerialization.enableThriftSerialization(conf);
		ProtoStuffSerialization.enableProtoStuffSerialization(conf);
		//TupleSerialization.enableTupleSerialization(conf);
		
		TupleInternalSerialization.enableSerialization(conf);
		
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
