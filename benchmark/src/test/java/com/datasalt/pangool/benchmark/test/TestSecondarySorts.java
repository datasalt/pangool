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
import org.junit.Test;

import com.datasalt.pangool.benchmark.secondarysort.CascadingSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.CrunchSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.HadoopSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.PangoolSecondarySort;
import com.datasalt.pangool.utils.HadoopUtils;

/**
 * This unit test verifies that each of the secondary sort example implementations can be run and that they give the
 * same output given a test input.
 */
public class TestSecondarySorts extends BaseBenchmarkTest {

	public final static String TEST_FILE = "src/test/resources/secondarysort/test-data.txt";
	public final static String EXPECTED_OUTPUT = "src/test/resources/secondarysort/expected-output.txt";
	public final static String OUT_PANGOOL = "src/test/resources/secondarysort/out-pangool-ss";
	public final static String OUT_CASCADING = "src/test/resources/secondarysort/out-cascading-ss";
	public final static String OUT_CRUNCH = "src/test/resources/secondarysort/out-crunch-ss";
	public final static String OUT_HADOOP = "src/test/resources/secondarysort/out-mapred-ss";

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
