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
import java.nio.charset.Charset;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestGrep extends AbstractHadoopTestLibrary {

	private final static String FOLDER = ".";
	private final static String INPUT = FOLDER + "/test-input-" + TestGrep.class.getName();
	private final static String OUTPUT = FOLDER + "/tests-files/test-output-" + TestGrep.class.getName();

	@Test
	public void test() throws Exception {

		Files.write("foo\nbar", new File(INPUT), Charset.forName("UTF-8"));
		ToolRunner.run(new Grep(), new String[] { "foo", INPUT, OUTPUT });
		assertEquals("foo", Files.toString(new File(OUTPUT + "/part-m-00000"), Charset.forName("UTF-8")).trim());

		trash(INPUT, OUTPUT);
	}
}
