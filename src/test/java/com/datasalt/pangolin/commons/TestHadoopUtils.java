package com.datasalt.pangolin.commons;

import static org.junit.Assert.assertEquals;

import java.io.IOException;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.commons.test.PangolinBaseTest;

/**
 * Test HadoopUtils class
 * 
 * @author ivan
 */
public class TestHadoopUtils extends PangolinBaseTest {

	@Test
	public void testStringToFile() throws IOException {
		FileSystem fs = FileSystem.getLocal(getConf());
		Path path = new Path(TestHadoopUtils.class.getCanonicalName());
		
		try {
			
			String text = "String\nDe Prueba";
			
			for (int i=0;i<10;i++) {
				text += text;
			}
			
			HadoopUtils.stringToFile(fs, path, text);
			String read = HadoopUtils.fileToString(fs, path);
			
			assertEquals(text, read);
		} finally {
			fs.delete(path, true);
		}
	}


}
