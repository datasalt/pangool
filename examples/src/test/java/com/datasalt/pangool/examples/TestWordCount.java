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
package com.datasalt.pangool.examples;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.examples.WordCount;
import com.datasalt.pangool.test.AbstractHadoopTestLibrary;
import com.datasalt.pangool.utils.HadoopUtils;
import com.google.common.io.Files;

public class TestWordCount extends AbstractHadoopTestLibrary{

	private final static String FOLDER = "/tmp";
	private final static String INPUT  = FOLDER + "/test-input-" + TestWordCount.class.getName();
	private final static String OUTPUT = FOLDER + "/test-output-" + TestWordCount.class.getName();
	
	@Test
	public void test() throws IOException,CoGrouperException, InterruptedException, ClassNotFoundException {
		Files.write("a b b c c c\nd d d d", new File(INPUT), Charset.forName("UTF-8"));
		Configuration conf = new Configuration();
		WordCount wordCount = new WordCount();
		Job job = wordCount.getJob(conf, INPUT, OUTPUT);
		assertRun(job);
		
		String[][] output = new String[4][];
		int count = 0;
		
		for(String line: Files.readLines(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8"))) {
			String[] fields = line.split("\t");
			output[count] = fields;
			count++;
		}
		
		assertEquals("a", output[0][0]);
		assertEquals("1", output[0][1]);
		
		assertEquals("b", output[1][0]);
		assertEquals("2", output[1][1]);
		
		assertEquals("c", output[2][0]);
		assertEquals("3", output[2][1]);
		
		assertEquals("d", output[3][0]);
		assertEquals("4", output[3][1]);
		
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(INPUT));
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT));
	}
}
