package com.datasalt.pangool.examples.grep;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.grep.Grep;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.google.common.io.Files;

public class TestGrep {

	private final static String INPUT = "test-input-" + TestGrep.class.getName();
	private final static String OUTPUT = "test-output-" + TestGrep.class.getName();

	@Test
	public void test() throws IOException, InvalidFieldException, CoGrouperException, InterruptedException,
	    ClassNotFoundException, URISyntaxException {
		
		Files.write("foo\nbar", new File(INPUT), Charset.forName("UTF-8"));
		Configuration conf = new Configuration();
		Grep grep = new Grep();
		grep.getJob(conf, "foo", INPUT, OUTPUT).waitForCompletion(true);
		assertEquals("foo", Files.toString(new File(OUTPUT + "/part-m-00000"), Charset.forName("UTF-8")).trim());
		
		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, new Path(INPUT));
		HadoopUtils.deleteIfExists(fS, new Path(OUTPUT));
	}
}
