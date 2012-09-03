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
package com.datasalt.pangool.examples.gameoflife;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestGameOfLifeJob extends AbstractHadoopTestLibrary {

	private final static String OUTPUT = "test-output-" + TestGameOfLifeJob.class.getName();

	@Test
	public void test() throws Exception {
		trash(OUTPUT);

		ToolRunner.run(getConf(),new GameOfLifeJob(), new String[] { OUTPUT, "2", "1" });
		
		/*
		 * For testing {@link GameOfLifeJob} we calculate all the possibilities in a 2x2 grid
		 * There are only 5 that converge.
		 */
		List<String> lines = Files.readLines(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8"));
		
		String[] spl = lines.get(0).split("\t");

		// 11
		// 10
		assertEquals("[0, 0, 0, 0, 0, 0, 0, 7]",  spl[0]);
		// Needs 2 iterations to converge
		assertEquals("2", spl[1]);
		
		// 11
		// 01
		spl = lines.get(1).split("\t");
		assertEquals("[0, 0, 0, 0, 0, 0, 0, 11]", spl[0]);
		// Needs 2 iterations to converge
		assertEquals("2", spl[1]);

		// 10
		// 11
		spl = lines.get(2).split("\t");
		assertEquals("[0, 0, 0, 0, 0, 0, 0, 13]", spl[0]);
		// Needs 2 iterations to converge
		assertEquals("2", spl[1]);

		// 01
		// 11
		spl = lines.get(3).split("\t");
		assertEquals("[0, 0, 0, 0, 0, 0, 0, 14]", spl[0]);
		// Needs 2 iterations to converge
		assertEquals("2", spl[1]);
		
		// 11
		// 11
		spl = lines.get(4).split("\t");
		assertEquals("[0, 0, 0, 0, 0, 0, 0, 15]", spl[0]);
		// Needs only 1 iteration to converge (already collapsed)
		assertEquals("1", spl[1]);
		
		trash(OUTPUT);
	}
}
