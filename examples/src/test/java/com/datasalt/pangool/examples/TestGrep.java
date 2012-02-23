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
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.examples.Grep;
import com.datasalt.pangool.utils.HadoopUtils;
import com.google.common.io.Files;

public class TestGrep {
	private final static String FOLDER = "/tmp";
	private final static String INPUT = FOLDER +"/test-input-" + TestGrep.class.getName();
	private final static String OUTPUT = FOLDER + "/tests-files/test-output-" + TestGrep.class.getName();

	@Test
	public void test() throws IOException, TupleMRException, InterruptedException,
	    ClassNotFoundException, URISyntaxException {
		
		Files.write("foo\nbar", new File(INPUT), Charset.forName("UTF-8"));
		Configuration conf = new Configuration();
		Grep grep = new Grep();
		grep.getJob(conf, "foo", INPUT, OUTPUT).waitForCompletion(true);
		assertEquals("foo", Files.toString(new File(OUTPUT + "/part-m-00000"), Charset.forName("UTF-8")).trim());
		
		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, new Path(INPUT));
		HadoopUtils.deleteIfExists(fS, new Path(OUTPUT));
	}
}
