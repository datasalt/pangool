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
package com.datasalt.pangool.examples.secondarysort;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.datasalt.pangool.examples.simplesecondarysort.SimpleSecondarySort;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestSecondarySort extends AbstractHadoopTestLibrary{

	private final static String INPUT = "test-input-" + TestSecondarySort.class.getName();
	private final static String OUTPUT = "test-output-" + TestSecondarySort.class.getName();

	@Test
	public void test() throws Exception {
		trash(OUTPUT);
		
		Files.write("10 3 \n 5 3 \n 5 30 \n 10 10", new File(INPUT), Charset.forName("UTF-8"));

		ToolRunner.run(new SimpleSecondarySort(), new String[] { INPUT, OUTPUT });

		String[][] expectedOutput = new String[][] { new String[] { "5", "3" }, new String[] { "5", "30" },
		    new String[] { "10", "3" }, new String[] { "10", "10" } };

		int count = 0;
		for(String line : Files.readLines(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8"))) {
			String[] fields = line.split("\t");
			assertEquals(fields[0], expectedOutput[count][0]);
			assertEquals(fields[1], expectedOutput[count][1]);
			count++;
		}

		trash(INPUT, OUTPUT);
	}
}
