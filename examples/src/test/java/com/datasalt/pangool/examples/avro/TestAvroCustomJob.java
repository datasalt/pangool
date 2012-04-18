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
package com.datasalt.pangool.examples.avro;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat.TupleInputReader;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestAvroCustomJob extends AbstractHadoopTestLibrary {
	
	public final static String INPUT = TestAvroCustomJob.class.getName() + "-input";
	public final static String OUTPUT = TestAvroCustomJob.class.getName() + "-output";
	
	@Test
	public void test() throws Exception {
		createInput(INPUT);
		ToolRunner.run( new AvroCustomSerializationJob(), new String[] {  INPUT, OUTPUT });
		
		assertEquals(6, assertOutput(OUTPUT + "/part-r-00000", new Configuration()));
		
		trash(INPUT, OUTPUT);
	}
	
	public static int assertOutput(String output, Configuration conf) throws NumberFormatException, IOException, InterruptedException {
		int validatedOutputLines = 0;
				
		TupleInputReader reader = new TupleInputReader(conf);
		reader.initialize(new Path(output), conf);
		while(reader.nextKeyValueNoSync()) {
			ITuple tuple = reader.getCurrentKey();
			Record record = (Record)tuple.get("my_avro");
			int topicId = (Integer) record.get("topic");
			String word = (record.get("word")).toString();
			int count   = (Integer) record.get("count");
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
