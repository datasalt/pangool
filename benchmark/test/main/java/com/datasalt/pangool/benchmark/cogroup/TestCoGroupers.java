package com.datasalt.pangool.benchmark.cogroup;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.commons.HadoopUtils;

/**
 * This unit test verifies that each of the URL Resolution cogroup implementations can be run and that they give the
 * same output given a test input.
 */
public class TestCoGroupers extends BaseBenchmarkTest {

	public final static String TEST_FILE_URL_MAP = "test/main/java/com/datasalt/pangool/benchmark/cogroup/url-map.txt";
	public final static String TEST_FILE_URL_REG = "test/main/java/com/datasalt/pangool/benchmark/cogroup/url-reg.txt";

	public final static String EXPECTED_OUTPUT = "test/main/java/com/datasalt/pangool/benchmark/cogroup/expected-output.txt";
	public final static String OUT_PANGOOL = "out-pangool-co";
	public final static String OUT_CASCADING = "out-cascading-co";
	public final static String OUT_CRUNCH = "out-crunch-co";
	public final static String OUT_MAPRED = "out-mapred-co";

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
		PangoolUrlResolution.main(new String[] { TEST_FILE_URL_MAP, TEST_FILE_URL_REG, OUT_PANGOOL });
		CascadingUrlResolution.main(new String[] { TEST_FILE_URL_MAP, TEST_FILE_URL_REG, OUT_CASCADING });
		CrunchUrlResolution.main(new String[] { TEST_FILE_URL_MAP, TEST_FILE_URL_REG, OUT_CRUNCH });
		HadoopUrlResolution.main(new String[] { TEST_FILE_URL_MAP, TEST_FILE_URL_REG, OUT_MAPRED });
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