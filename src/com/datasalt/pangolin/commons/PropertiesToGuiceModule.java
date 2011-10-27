package com.datasalt.pangolin.commons;

import java.util.Properties;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * A module that binds all the properties to named 
 * String bindings. 
 * 
 * @author ivan
 *
 */
public class PropertiesToGuiceModule extends AbstractModule {

	private Properties prop;
	
	public PropertiesToGuiceModule(Properties prop) {
		this.prop = prop;
	}

	@Override
  protected void configure() {
		for (Object okey: prop.keySet()) {
			String key = (String) okey;
			bind(String.class).annotatedWith(Names.named(key)).toInstance(prop.getProperty(key));
		}
  }
		
}
