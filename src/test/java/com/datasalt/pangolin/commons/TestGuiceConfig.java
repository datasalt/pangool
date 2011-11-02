package com.datasalt.pangolin.commons;

import junit.framework.Assert;


import org.junit.Test;

import com.datasalt.pangolin.commons.test.PangolinBaseTest;

/**
 * Guice configuration test : This test was inspired by a bug in my ide. If it fails, you may have to remove conf/ from
 * src path (in eclipse properties) and add it to libraries. Unfortunately, this test might not detect ide errors.
 * 
 * @author Jpeerindex
 * 
 */
public class TestGuiceConfig extends PangolinBaseTest {
	@Test
	public void test() {
		try {
			new PangolinGuiceModule();
		} catch(Throwable t) {
			Assert.fail("Guice is unconfigured.  (Possibly sure conf is added as a library, and remove from classpath)... Exception : "
			        + t);

		}
	}
}
