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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestTopicalWordCount extends AbstractHadoopTestLibrary {
	
	public final static String INPUT = TestTopicalWordCount.class.getName() + "-input";
	public final static String OUTPUT = TestTopicalWordCount.class.getName() + "-output";
	
	@Test
	public void test() throws Exception {
		createInput(INPUT);
		ToolRunner.run(getConf(), new TopicalWordCount(), new String[] {  INPUT, OUTPUT });
		
		assertEquals(6, assertOutput(OUTPUT + "/part-r-00000", new Configuration()));
		
		trash(INPUT, OUTPUT);
	}
	
	public static int assertOutput(String output, Configuration conf) throws NumberFormatException, IOException, InterruptedException {
		int validatedOutputLines = 0;

    Path outPath = new Path(output);
    TupleFile.Reader reader = new TupleFile.Reader(FileSystem.get(outPath.toUri(), conf), conf, outPath);
    Tuple tuple = new Tuple(reader.getSchema());

		while(reader.next(tuple)) {
			int topicId = (Integer) tuple.get("topic");
			String word = ((Utf8) tuple.get("word")).toString();
			int count   = (Integer) tuple.get("count");
			if(topicId == 1) {
				if(word.equals("bar") || word.equals("foo")) {
					assertEquals(2, count);
					validatedOutputLines++;
				} else if(word.equals("blah") || word.equals("bloh")) {
					assertEquals(1, count);
					validatedOutputLines++;
				}
			} else if(topicId == 2) {
				if(word.equals("bar")) {
					assertEquals(2, count);
					validatedOutputLines++;
				} else if(word.equals("bor")) {
					assertEquals(1, count);
					validatedOutputLines++;
				}				
			}
		}
		reader.close();
		return validatedOutputLines;
	}
	
	public static void createInput(String where) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(where));
		ObjectMapper jsonMapper = new ObjectMapper();
		Map<String, Object> jsonEntry = new HashMap<String, Object>();
		jsonEntry.put("text", "foo bar bar");
		jsonEntry.put("topicId", 1);
		
		writer.write(jsonMapper.writeValueAsString(jsonEntry) + "\n");
		
		jsonEntry.put("text", "foo blah bloh");
		jsonEntry.put("topicId", 1);
		
		writer.write(jsonMapper.writeValueAsString(jsonEntry) + "\n");

		jsonEntry.put("text", "bar bar bor");
		jsonEntry.put("topicId", 2);

		writer.write(jsonMapper.writeValueAsString(jsonEntry) + "\n");

		writer.close();
	}
}
