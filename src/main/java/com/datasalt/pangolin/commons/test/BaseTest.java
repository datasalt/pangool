package com.datasalt.pangolin.commons.test;

import java.io.IOException;
import java.util.HashMap;



import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;

import com.datasalt.pangolin.commons.PangolinConfigurationFactory;
import com.datasalt.pangolin.commons.PangolinGuiceModule;
import com.datasalt.pangolin.io.Serialization;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

public class BaseTest {

	public final static TypeReference<HashMap<String, Object>> MAP = new TypeReference<HashMap<String, Object>>() {
	};
	protected ObjectMapper mapper = new ObjectMapper();

	@Inject protected PangolinConfigurationFactory pisaeConfiguration;
	protected Serialization ser; 
	protected Injector injector;

	public Serialization getSer() throws IOException {
		if (ser == null) {
			ser = new Serialization(getConf());	
		}
		return ser;
	}

	public Configuration getConf() throws IOException {
		return pisaeConfiguration.create();
	}

	@Before
	public void prepare() throws IOException {
		injector = Guice.createInjector(new PangolinGuiceModule());
		injector.injectMembers(this);

		ser = new Serialization(getConf());
	}
}