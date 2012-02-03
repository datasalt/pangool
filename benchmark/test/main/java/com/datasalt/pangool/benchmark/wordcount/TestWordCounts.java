package com.datasalt.pangool.benchmark.wordcount;

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
 * This unit test verifies that each of the word count implementations can be run and that they give the same output
 * given a test input.
 */
public class TestWordCounts extends BaseBenchmarkTest {

	public final static String TEST_FILE = "test/main/java/com/datasalt/pangool/benchmark/wordcount/words.txt";

	public final static String EXPECTED_OUTPUT = "test/main/java/com/datasalt/pangool/benchmark/wordcount/expected-output.txt";
	public final static String OUT_PANGOOL = "out-pangool-wc";
	public final static String OUT_CASCADING = "out-cascading-wc";
	public final static String OUT_CRUNCH = "out-crunch-wc";
	public final static String OUT_MAPRED = "out-mapred-wc";

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
		WordCount.main(new String[] { TEST_FILE, OUT_PANGOOL });
		CascadingWordCount.main(new String[] { TEST_FILE, OUT_CASCADING });
		CrunchWordCount.main(new String[] { TEST_FILE, OUT_CRUNCH });
		MapRedWordCount.main(new String[] { TEST_FILE, OUT_MAPRED });
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
