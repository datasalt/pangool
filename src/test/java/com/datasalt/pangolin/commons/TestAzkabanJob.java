package com.datasalt.pangolin.commons;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.PangolinBaseTest;

/**
 * <p>Tests the integration between {@link BaseJob} and Azkaban through {@link AzkabanJob} by simulating how Azkaban would use it.</p>
 * 
 * @author pere
 *
 */
public class TestAzkabanJob extends PangolinBaseTest {
	
	@Before
	@After
	public void prepare() throws IOException {
		super.prepare();
		
		Path fakeIn = new Path("/tmp/fakein");
		Path fakeOut = new Path("/tmp/fakeout");
		
		FileSystem fS = FileSystem.get(getConf());
		HadoopUtils.deleteIfExists(fS, fakeIn);
		HadoopUtils.deleteIfExists(fS, fakeOut);
		fS.mkdirs(fakeIn);
	}
	
	@Test
	public void test() throws Exception {
		/*
		 * Hypothetical configuration
		 */
		Properties props = new Properties();
		props.setProperty(AzkabanJob.JOB_CLASS_NAME, CoffeeJob.class.getName());
		props.setProperty(AzkabanJob.JOB_CLASS_ARGS, "/tmp/fakein /tmp/fakeout");
		props.setProperty(AzkabanJob.JOB_HADOOP_ARGS,"-D mapred.reduce.tasks=2");
		final AzkabanJob job = new AzkabanJob("Coffe job", props);
		/*
		 * Let's try to imitate what Azkaban would do with our Job
		 */
		final AtomicBoolean fail = new AtomicBoolean(false);
		/*
		 * A thread runs the job
		 */
		Thread runner = new Thread() {
			public void run() {
				try {
	        job.run();
        } catch (Exception e) {
        	e.printStackTrace();
        	fail.set(true);
        }
			}
		};
		runner.start();
		/*
		 * Another thread checks the status of it
		 */
		while(runner.isAlive() && !fail.get()) {
			Thread.sleep(50);
		}
		if(fail.get()) {
			throw new RuntimeException("Job failed");
		}
	}
}
