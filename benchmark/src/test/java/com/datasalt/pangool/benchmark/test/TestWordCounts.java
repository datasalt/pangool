/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.benchmark.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.datasalt.pangool.benchmark.wordcount.CascadingWordCount;
import com.datasalt.pangool.benchmark.wordcount.CrunchWordCount;
import com.datasalt.pangool.benchmark.wordcount.HadoopWordCount;
import com.datasalt.pangool.benchmark.wordcount.PangoolWordCount;
import com.datasalt.pangool.utils.HadoopUtils;

/**
 * This unit test verifies that each of the word count implementations can be run and that they give the same output
 * given a test input.
 */
public class TestWordCounts extends BaseBenchmarkTest {

	private final static String TEST_FILE = "src/test/resources/wordcount/words.txt";
	private final static String EXPECTED_OUTPUT = "src/test/resources/wordcount/expected-output.txt";
	
	private static final String OUTPUT_FOLDER = "/tmp/";
	private final static String OUT_PANGOOL = OUTPUT_FOLDER + "/out-pangool-wc";
	private final static String OUT_CASCADING = OUTPUT_FOLDER + "/out-cascading-wc";
	private final static String OUT_CRUNCH = OUTPUT_FOLDER + "out-crunch-wc";
	private final static String OUT_MAPRED = OUTPUT_FOLDER + "out-mapred-wc";

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
	public void testHadoop() throws Exception {
		HadoopWordCount.main(new String[] { TEST_FILE, OUT_MAPRED });
		String outMapred = getReducerOutputAsText(OUT_MAPRED);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(outMapred, expectedOutput);
	}
	
	@Test
	public void testPangool() throws Exception {
		PangoolWordCount.main(new String[] { TEST_FILE, OUT_PANGOOL });
		String outPangool = getReducerOutputAsText(OUT_PANGOOL);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(outPangool, expectedOutput);
	}
	
	@Test
	public void testCascading() throws Exception {
		CascadingWordCount.main(new String[] { TEST_FILE, OUT_CASCADING });
		String outCascading = getOutputAsText(OUT_CASCADING + "/part-00000");
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput, outCascading);
	}
	
	@Ignore
	@Test
	public void testCrunch() throws Exception {
		CrunchWordCount.main(new String[] { TEST_FILE, OUT_CRUNCH });
		String outCrunch = getReducerOutputAsText(OUT_CRUNCH);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput, outCrunch);
	}
	
}
