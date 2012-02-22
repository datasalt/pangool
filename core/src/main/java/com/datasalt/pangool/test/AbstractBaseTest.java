package com.datasalt.pangool.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;

import com.datasalt.pangool.serialization.protostuff.ProtoStuffSerialization;
import com.datasalt.pangool.serialization.thrift.ThriftSerialization;
import com.datasalt.pangool.serialization.tuples.PangoolSerialization;

public abstract class AbstractBaseTest {
	
	private Configuration conf;

	public Configuration getConf() throws IOException {
		if (conf == null){
			conf =createConf();
		}
		return conf;
	}

	public static Configuration createNewConfiguration() {
		Configuration conf = new Configuration();
		return conf;
	}
	
	private static void configureSerialization(Configuration conf) {
		ThriftSerialization.enableThriftSerialization(conf);
		ProtoStuffSerialization.enableProtoStuffSerialization(conf);		
		PangoolSerialization.enableSerialization(conf);
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
