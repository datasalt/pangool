package com.datasalt.pangool.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;

import com.datasalt.pangool.io.tuple.ser.TupleInternalSerialization;
import com.datasalt.pangool.serialization.protostuff.ProtoStuffSerialization;
import com.datasalt.pangool.serialization.thrift.ThriftSerialization;

public abstract class AbstractBaseTest {
	
	private Configuration conf;

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
