package com.datasalt.pangool.examples.wordcount;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.google.common.io.Files;

public class TestWordCount {

	private final static String INPUT  = "test-input-" + TestWordCount.class.getName();
	private final static String OUTPUT = "test-output-" + TestWordCount.class.getName();
	
	@Test
	public void test() throws IOException, InvalidFieldException, CoGrouperException, InterruptedException, ClassNotFoundException {
		Files.write("a b b c c c", new File(INPUT), Charset.forName("UTF-8"));
		Configuration conf = new Configuration();
		WordCount wordCount = new WordCount();
		wordCount.getJob(conf, INPUT, OUTPUT).waitForCompletion(true);
		System.out.println(Files.toString(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8")));
	}
}
