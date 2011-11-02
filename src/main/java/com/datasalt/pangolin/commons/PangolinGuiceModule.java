package com.datasalt.pangolin.commons;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * Guice Module Class for the Pisae project
 * 
 * This class defines the bindings performed by the Guice injector in the project.
 * 
 * It also load the configuration properties files and prepares them to be binded with properties anywhere in the app
 * with commands as @Inject @Name("key.name") String configValue;
 * 
 * For instantiate any class in the Pisae project, the following pattern is recommended:
 * 
 * <code>
 * 		Injector injector = Guice.createInjector(new PisaeGuiceModule());
 * 		MyClass c = injector.getInstance(MyClass.class);
 * </code>
 * 
 * @author ivan,epalace
 */
public class PangolinGuiceModule extends AbstractModule {

	
	private static Logger log = LoggerFactory.getLogger(PangolinGuiceModule.class);
	

	public Properties getConfigProperties() {
		return configProperties;
	}

	private Properties configProperties = new Properties();

	// named annotation used to check if an object has been or not injected
	public final static String IS_INJECTED = "is.injected";

	public PangolinGuiceModule() {
		load("project.properties", true);
	}

	/**
	 * Configure here all the bindings needed for the injections of the configurations, and other objects with Guice.
	 */
	@Override
	protected void configure() {
		bindConfigurationStrings();
	}

	protected void bindConfigurationStrings() {

		// Injection used to check whether an object has been injected or not
		bind(Boolean.class).annotatedWith(Names.named(IS_INJECTED)).toInstance(true);

		for(Object key : configProperties.keySet()) {
			bind(String.class).annotatedWith(Names.named((String) key))
			    .toInstance(configProperties.getProperty((String) key));
		}
    
	}

	/**
	 * Loads property files that latter will be used for binding.
	 */
	protected boolean load(String what, boolean failIfNotFound) {
		URL url = Thread.currentThread().getContextClassLoader().getResource(what);
		if(url == null) {
			if(failIfNotFound) {
				throw new RuntimeException(
				    what
				        + " not in classpath. If using eclipse, go to Project Properties, Libraries-> Add Class folder -> and add /conf to your classpath");
			} else {
				log.info(what + " not in classpath.");

			}
		} else {
			log.info("Loading " + what + " from " + url);

			InputStream inStream;
			try {
				inStream = new FileInputStream(new File(url.toURI()));
				configProperties.load(inStream);
				inStream.close();
			} catch(Exception e) {

				throw new RuntimeException("Error loading configuration from " + what, e);
			}
		}
		return true;
	}

}

