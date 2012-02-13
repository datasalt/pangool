package com.datasalt.pangool.benchmark.secondarysort;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.benchmark.cogroup.BaseBenchmarkTest;
import com.datasalt.pangool.commons.HadoopUtils;

/**
 * This unit test verifies that each of the secondary sort example implementations can be run and that they give the
 * same output given a test input.
 */
public class TestSecondarySorts extends BaseBenchmarkTest {

	public final static String TEST_FILE = "test/main/java/com/datasalt/pangool/benchmark/secondarysort/test-data.txt";
	public final static String EXPECTED_OUTPUT = "test/main/java/com/datasalt/pangool/benchmark/secondarysort/expected-output.txt";
	public final static String OUT_PANGOOL = "out-pangool-ss";
	public final static String OUT_CASCADING = "out-cascading-ss";
	public final static String OUT_CRUNCH = "out-crunch-ss";
	public final static String OUT_HADOOP = "out-mapred-ss";

	@Before
	@After
	public void prepare() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, new Path(OUT_PANGOOL));
		HadoopUtils.deleteIfExists(fS, new Path(OUT_CASCADING));
		HadoopUtils.deleteIfExists(fS, new Path(OUT_CRUNCH));
		HadoopUtils.deleteIfExists(fS, new Path(OUT_HADOOP));
	}

	
	@Test
	public void testHadoop() throws Exception {
		HadoopSecondarySort.main(new String[] { TEST_FILE, OUT_HADOOP });
		String outMapred = getReducerOutputAsText(OUT_HADOOP);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput,outMapred);
	}
	
	@Test
	public void testPangool() throws Exception {
		PangoolSecondarySort.main(new String[] { TEST_FILE, OUT_PANGOOL });
		String outPangool = getReducerOutputAsText(OUT_PANGOOL);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput,outPangool);
	}
	
	@Test
	public void testCascading() throws Exception {
		CascadingSecondarySort.main(new String[] { TEST_FILE, OUT_CASCADING });
		String out = getOutputAsText(OUT_CASCADING + "/part-00000");
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput,out);
	}
	
	@Test
	public void testCrunch() throws Exception {
		CrunchSecondarySort.main(new String[] { TEST_FILE, OUT_CRUNCH });
		String outCrunch = getReducerOutputAsText(OUT_CRUNCH);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput,outCrunch);
	}
	
}
