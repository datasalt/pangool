package com.datasalt.pangolin.commons.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;

import com.datasalt.pangolin.io.Serialization;



public abstract class AbstractBaseTest {

	//public final static TypeReference<HashMap<String, Object>> MAP = new TypeReference<HashMap<String, Object>>() {
	//};
	//protected ObjectMapper mapper = new ObjectMapper();

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
			conf =new Configuration(); //TODO change this 
		}
		return conf;
	}

	
	//public abstract AbstractModule getGuiceModule();
	//public abstract Provider<Configuration> getProvider();
	
	@Before
	public void prepare() throws IOException {

	}
	
}
