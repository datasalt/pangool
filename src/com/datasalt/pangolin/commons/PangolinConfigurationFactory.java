package com.datasalt.pangolin.commons;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasalt.pangolin.commons.conf.ProjectConf;
import com.datasalt.pangolin.commons.io.ProtoStuffSerialization;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Singleton;

/**
 * <p>This class provides a factory for instantiating Hadoop Configuration objects in a way such that all the libs and
 * configuration of the project will be put in the DistributedCache, so that Mappers and Reducers will have them in their JVMs.</p>
 * <p>It also provides the synchronize() method which should be called to upload the libs/ and conf/ to the HDFS when they change.</p> 
 * 
 * @author pere
 *
 */
@Singleton
public class PangolinConfigurationFactory implements Provider<Configuration> {
	
	static Logger log = LoggerFactory.getLogger(PangolinConfigurationFactory.class);

	protected ProjectConf projectConf;
	
	@Inject
	public PangolinConfigurationFactory(ProjectConf projectConf) {
		this.projectConf = projectConf;
	}
	
	public void synchronize() throws IOException {
		String projectName = projectConf.getProjectName();
		Configuration conf = new Configuration();
		FileSystem dFs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		Path libPath  = new Path("/" + projectName + "/lib");
		Path confPath = new Path("/" + projectName + "/conf");
		Path localLibPath = new Path("lib");
		Path localConfPath = new Path("conf");
		log.info("Synchronizing " + localLibPath + " to " + libPath);		
    HadoopUtils.synchronize(local, localLibPath, dFs, libPath);
		log.info("Synchronizing " + localConfPath + " to " + confPath);
    HadoopUtils.synchronize(local, localConfPath, dFs, confPath);    
	}
	
	public void populate(Configuration conf) throws IOException {
		FileSystem dFs = FileSystem.get(conf);
		if(conf.get("fs.default.name").startsWith("file:")) {
			return;
		}

		String projectName = projectConf.getProjectName();
		Path libPath  = new Path("/" + projectName + "/lib");
		Path confPath = new Path("/" + projectName + "/conf");
		/*
		 * Add config folder to classpath
		 */
		log.info("Adding HDFS Path " + confPath + " to classpath");
		DistributedCache.addFileToClassPath(confPath, conf);
		FileStatus[] libraries = dFs.listStatus(libPath);
		log.info("Adding all files in " + libPath + " to classpath");
		for(FileStatus library: libraries) {
			/*
			 * Add each JAR to classpath - tiene su truco! =|
			 */
			DistributedCache.addFileToClassPath(new Path(libPath, library.getPath().getName()), conf);
		}				
	}
	
	public void configureSerialization(Configuration conf) {
		// Adding the Thrift serialization
		String ser = conf.get("io.serializations").trim();
		if (ser.length() !=0 ) {
			ser += ",";
		}
		ser += "org.apache.hadoop.contrib.serialization.thrift.ThriftSerialization";
		ser += "," + ProtoStuffSerialization.class.getName();
		conf.set("io.serializations", ser);
	}
	
	public Configuration create() throws IOException {
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");		
		Configuration conf = new Configuration();
		
		/*
		 * TODO Investigate why we can't do this without instantiating the configuration twice 
		 */
		
//		for(String property : new String[] { "mapred.child.java.opts", "mapred.map.child.java.opts", "mapred.reduce.child.java.opts" }) {
//			String mapredChildJavaOpts = conf.get(property, "") + " " +
//			 "-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl";
//			conf.set(property, mapredChildJavaOpts.trim());
//			log.info("Setting [" + property + "] to [" + mapredChildJavaOpts.trim() + "]");
//		}
		
		populate(conf);
		configureSerialization(conf);
		configureSnappyCompression(conf);
		return conf;
	}
	
	private void configureSnappyCompression(Configuration conf) {
		
		String[] codecs = conf.getStrings("io.compression.codecs");
		boolean found = false;
		for (String codec: codecs) {
			if (codec.equals("org.apache.hadoop.io.compress.SnappyCodec")) {
				log.info("Snappy compression support detected. Enabling it, so all outputs are compressed...");

				// Compress Map output
				conf.set("mapred.compress.map.output","true");
				conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");

				// Compress MapReduce output
				conf.set("mapred.output.compress","true");
				conf.set("mapred.output.compression","org.apache.hadoop.io.compress.SnappyCodec");
				
				found = true;
				break;
			}
		}			
		if(!found) {
			log.info("Snappy compression not enabled because it was not found. Continue without it");
		}
	}

	public static void main(String[] args) throws IOException {
		Injector injector = Guice.createInjector(new PangolinGuiceModule());
		injector.getInstance(PangolinConfigurationFactory.class).synchronize();
	}

	@Override
  public Configuration get() {
	  try {
	    return create();
    } catch(IOException e) {
	    throw new RuntimeException(e);
    }
  }
}