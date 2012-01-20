package com.datasalt.pangool.examples.secondarysort;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.processor.Processor;
import com.google.common.io.Files;

public class TestSecondarySort {
	private final static String INPUT = "test-input-" + TestSecondarySort.class.getName();
	private final static String OUTPUT = "test-output-" + TestSecondarySort.class.getName();

	@Test
	public void test() throws IOException, InvalidFieldException, CoGrouperException, InterruptedException,
	    ClassNotFoundException, URISyntaxException {

		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, new Path(OUTPUT));
		Files.write("10 3 \n 5 3 \n 5 30 \n 10 10", new File(INPUT), Charset.forName("UTF-8"));

		SecondarySort sSort = new SecondarySort();
		sSort.getJob(conf, INPUT, OUTPUT).waitForCompletion(true);

		String[][] expectedOutput = new String[][] { new String[] { "5", "3" }, new String[] { "5", "30" },
		    new String[] { "10", "3" }, new String[] { "10", "10" } };

		int count = 0;
		for(String line : Files.readLines(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8"))) {
			String[] fields = line.split("\t");
			assertEquals(fields[0], expectedOutput[count][0]);
			assertEquals(fields[1], expectedOutput[count][1]);
			count++;
		}

		HadoopUtils.deleteIfExists(fS, new Path(Processor.SERIALIZED_HANDLER_LOCAL_FILE));
		HadoopUtils.deleteIfExists(fS, new Path(INPUT));
		HadoopUtils.deleteIfExists(fS, new Path(OUTPUT));
	}
}
