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
	public final static String OUT_MAPRED = "out-mapred-ss";

	@Before
	@After
	public void prepare() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, new Path(OUT_PANGOOL));
		HadoopUtils.deleteIfExists(fS, new Path(OUT_CASCADING));
		HadoopUtils.deleteIfExists(fS, new Path(OUT_CRUNCH));
		HadoopUtils.deleteIfExists(fS, new Path(OUT_MAPRED));
	}

	@Test
	public void test() throws Exception {
		PangoolSecondarySort.main(new String[] { TEST_FILE, OUT_PANGOOL });
		CascadingSecondarySort.main(new String[] { TEST_FILE, OUT_CASCADING });
		CrunchSecondarySort.main(new String[] { TEST_FILE, OUT_CRUNCH });
		HadoopSecondarySort.main(new String[] { TEST_FILE, OUT_MAPRED });
		String outPangool = getReducerOutputAsText(OUT_PANGOOL);
		String outCascading = getOutputAsText(OUT_CASCADING + "/part-00000");
		String outCrunch = getReducerOutputAsText(OUT_CRUNCH);
		String outMapred = getReducerOutputAsText(OUT_MAPRED);

		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);

		assertEquals(outPangool, expectedOutput);

		assertEquals(outPangool, outCascading);
		assertEquals(outPangool, outCrunch);
		assertEquals(outPangool, outMapred);
	}
}
