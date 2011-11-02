package com.datasalt.pangolin.commons.test;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;

import com.datasalt.pangolin.io.Serialization;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;

public abstract class AbstractBaseTest {

	
	public final static TypeReference<HashMap<String, Object>> MAP = new TypeReference<HashMap<String, Object>>() {
	};
	protected ObjectMapper mapper = new ObjectMapper();

	//@Inject	protected Provider<Configuration> configurationFactory;
	private Configuration conf;
	protected Serialization ser; 
	protected Injector injector;

	public Serialization getSer() throws IOException {
		if (ser == null) {
			ser = new Serialization(getConf());	
		}
		return ser;
	}

	public Configuration getConf() throws IOException {
		if (conf == null){
			conf =getProvider().get(); 
		}
		return conf;
	}

	
	public abstract AbstractModule getGuiceModule();
	public abstract Provider<Configuration> getProvider();
	
	@Before
	public void prepare() throws IOException {
		injector = Guice.createInjector(getGuiceModule());
		injector.injectMembers(this);
		
		ser = new Serialization(getConf());
	}
}
