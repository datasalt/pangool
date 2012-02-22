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
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.benchmark.cogroup.CascadingUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.CrunchUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.HadoopUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.PangoolUrlResolution;
import com.datasalt.pangool.utils.HadoopUtils;

/**
 * This unit test verifies that each of the URL Resolution cogroup implementations can be run and that they give the
 * same output given a test input.
 */
public class TestCoGroupers extends BaseBenchmarkTest {

	private final static String TEST_FILE_URL_MAP = "src/test/resources/cogroup/url-map.txt";
	private final static String TEST_FILE_URL_REG = "src/test/resources/cogroup/url-reg.txt";

	private final static String EXPECTED_OUTPUT = "src/test/resources/cogroup/expected-output.txt";
	
	private final static String OUTPUT_FOLDER = "/tmp";
	private final static String OUT_PANGOOL = OUTPUT_FOLDER + "/out-cogroup-pangool-co";
	private final static String OUT_CASCADING = OUTPUT_FOLDER + "/out-cogroup-cascading-co";
	private final static String OUT_CRUNCH = OUTPUT_FOLDER + "/out-cogroup-crunch-co";
	private final static String OUT_MAPRED = OUTPUT_FOLDER + "/out-cogroup-hadoop-co";

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
		HadoopUrlResolution.main(new String[] { TEST_FILE_URL_MAP, TEST_FILE_URL_REG, OUT_MAPRED });
		String outMapred = getReducerOutputAsText(OUT_MAPRED);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput, outMapred);
	}
	
	@Test
	public void testPangool() throws Exception {
		Logger root = Logger.getRootLogger();
		root.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
		PangoolUrlResolution.main(new String[] { TEST_FILE_URL_MAP, TEST_FILE_URL_REG, OUT_PANGOOL });
		String outPangool = getReducerOutputAsText(OUT_PANGOOL);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput,outPangool);
	}
	
	@Test
	public void testCascading() throws Exception {
		CascadingUrlResolution.main(new String[] { TEST_FILE_URL_MAP, TEST_FILE_URL_REG, OUT_CASCADING });
		String outCascading = getOutputAsText(OUT_CASCADING + "/part-00000");
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput, outCascading);
	}
	
	@Test
	public void testCrunch() throws Exception {
		CrunchUrlResolution.main(new String[] { TEST_FILE_URL_MAP, TEST_FILE_URL_REG, OUT_CRUNCH });
		String outCrunch = getReducerOutputAsText(OUT_CRUNCH);
		String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
		assertEquals(expectedOutput, outCrunch);
	}
	
	
}