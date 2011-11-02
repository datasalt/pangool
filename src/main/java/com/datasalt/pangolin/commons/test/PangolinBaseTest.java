package com.datasalt.pangolin.commons.test;

import org.apache.hadoop.conf.Configuration;

import com.datasalt.pangolin.commons.PangolinConfigurationFactory;
import com.datasalt.pangolin.commons.PangolinGuiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class PangolinBaseTest extends AbstractBaseTest{

	@Inject	PangolinConfigurationFactory factory;
	
	@Override
  public AbstractModule getGuiceModule() {
	  return new PangolinGuiceModule();
  }

	@Override
  public Provider<Configuration> getProvider() {
	  return factory;
  }

	

	
}