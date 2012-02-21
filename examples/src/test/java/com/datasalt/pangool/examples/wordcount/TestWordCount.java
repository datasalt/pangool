package com.datasalt.pangool.examples.wordcount;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.commons.HadoopUtils;
import com.google.common.io.Files;

public class TestWordCount {

	private final static String INPUT  = "test-input-" + TestWordCount.class.getName();
	private final static String OUTPUT = "test-output-" + TestWordCount.class.getName();
	
	@Test
	public void test() throws IOException,CoGrouperException, InterruptedException, ClassNotFoundException {
		Files.write("a b b c c c\nd d d d", new File(INPUT), Charset.forName("UTF-8"));
		Configuration conf = new Configuration();
		WordCount wordCount = new WordCount();
		wordCount.getJob(conf, INPUT, OUTPUT).waitForCompletion(true);
		
		String[][] output = new String[4][];
		int count = 0;
		
		for(String line: Files.readLines(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8"))) {
			String[] fields = line.split("\t");
			output[count] = fields;
			count++;
		}
		
		assertEquals("a", output[0][0]);
		assertEquals("1", output[0][1]);
		
		assertEquals("b", output[1][0]);
		assertEquals("2", output[1][1]);
		
		assertEquals("c", output[2][0]);
		assertEquals("3", output[2][1]);
		
		assertEquals("d", output[3][0]);
		assertEquals("4", output[3][1]);
		
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(INPUT));
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT));
	}
}
