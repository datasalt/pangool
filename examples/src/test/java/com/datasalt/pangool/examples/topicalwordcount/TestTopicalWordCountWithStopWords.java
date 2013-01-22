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
package com.datasalt.pangool.examples.topicalwordcount;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestTopicalWordCountWithStopWords extends AbstractHadoopTestLibrary {
	
	private final static String STOP_WORDS = TestTopicalWordCountWithStopWords.class.getName() + "-stop-words.txt";
	private final static String INPUT = TestTopicalWordCountWithStopWords.class.getName() + "-input";
	private final static String OUTPUT = TestTopicalWordCountWithStopWords.class.getName() + "-output";
	
	@Test
	public void test() throws Exception {
        trash(INPUT, STOP_WORDS, OUTPUT);
		
		Configuration conf = new Configuration();
		
		TestTopicalWordCount.createInput(INPUT);

		Files.touch(new File(STOP_WORDS));
		ToolRunner.run(getConf(), new TopicalWordCountWithStopWords(), new String[] { INPUT, OUTPUT, STOP_WORDS });
		
		assertEquals(6, TestTopicalWordCount.assertOutput(OUTPUT + "/part-r-00000", conf));
		
		// Stop words: bar, bloh
		Files.write(("bar" + "\n" + "bloh").getBytes("UTF-8"), new File(STOP_WORDS));
	    trash(OUTPUT);
		ToolRunner.run(getConf(), new TopicalWordCountWithStopWords(), new String[] { INPUT, OUTPUT, STOP_WORDS });

		assertEquals(3, TestTopicalWordCount.assertOutput(OUTPUT + "/part-r-00000", conf));
		
		trash(INPUT, STOP_WORDS, OUTPUT);
	}
}
