package com.datasalt.pangool.examples.normalization;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.utils.HadoopUtils;
import com.google.common.io.Files;

public class TestUserActivityNormalizer {

	private final static String INPUT = "test-input-" + TestUserActivityNormalizer.class.getName();
	private final static String OUTPUT = "test-output-" + TestUserActivityNormalizer.class.getName();
	
	@Test
	public void test() throws IOException, CoGrouperException, InterruptedException,
	    ClassNotFoundException, URISyntaxException, ParseException {

		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, new Path(OUTPUT));
		Files.write(
				"user1" + "\t" + "feat1" + "\t" + "10" + "\n" +
				"user1" + "\t" + "feat1" + "\t" + "20" + "\n" +
				"user1" + "\t" + "feat2" + "\t" + "30" + "\n" +
				"user2" + "\t" + "feat1" + "\t" + "10" + "\n" +
				"user2" + "\t" + "feat2" + "\t" + "10" + "\n" +
				"user2" + "\t" + "feat3" + "\t" + "10" + "\n"
		, new File(INPUT), Charset.forName("UTF-8"));

		UserActivityNormalizer normalizer = new UserActivityNormalizer();
		normalizer.getJob(conf, INPUT, OUTPUT).waitForCompletion(true);
		
	}
}
