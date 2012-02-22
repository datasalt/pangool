package com.datasalt.pangool.benchmark.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.benchmark.utf8sorting.PangoolAccentsCustomComparator;
import com.datasalt.pangool.benchmark.utf8sorting.Utf8EncodedRepeatedField;
import com.datasalt.pangool.commons.HadoopUtils;

/**
 * This unit test verifies that each of the word count implementations can be run and that they give the same output
 * given a test input.
 */
public class TestSansAccentSorting extends BaseBenchmarkTest {

	private final static String FOLDER = "src/test/resources/sans_accent_sorting";
	private final static String TEST_FILE = FOLDER + "/spanish_words.txt";

	private final static String EXPECTED_OUTPUT = FOLDER + "/expected.txt";
	private final static String OUT_REPEATING = FOLDER + "/out-pangool-repeating";
	private final static String OUT_CUSTOM_COMPARATOR = FOLDER + "/out-pangool-repeating";
	

	@Before
	@After
	public void prepare() throws IOException {
		Logger root = Logger.getRootLogger();
		root.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, new Path(OUT_REPEATING));
		HadoopUtils.deleteIfExists(fS, new Path(OUT_CUSTOM_COMPARATOR));
		
	}

	@Test
	public void testRepeatingFields() throws Exception {
		Configuration conf = new Configuration();
		Job job = new Utf8EncodedRepeatedField().getJob(conf,TEST_FILE,OUT_REPEATING);
		assertRun(job);
		
		String out = getReducerOutputAsText(OUT_REPEATING); //Very bad, consumes a lot of memory
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);//Very bad, consumes a lot of memory
		assertEquals(expectedOutput,out);
	}
	
	@Test
	public void testCustomComparator() throws Exception {
		Configuration conf = new Configuration();
		Job job = new PangoolAccentsCustomComparator().getJob(conf,TEST_FILE, OUT_CUSTOM_COMPARATOR);
		assertRun(job);
		
		String out = getReducerOutputAsText(OUT_CUSTOM_COMPARATOR);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput,out);
	}
	
	
	
}
