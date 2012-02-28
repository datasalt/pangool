package com.datasalt.pangool.examples.topicalwordcount;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.io.TupleInputFormat.TupleInputReader;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.tuple.ITuple;

public class TestTopicalWordCount {

	public final static String INPUT = TestTopicalWordCount.class.getName() + "-input";
	public final static String OUTPUT = TestTopicalWordCount.class.getName() + "-output";
	
	@Test
	public void test() throws Exception {
		createInput(INPUT);
		ToolRunner.run( new TopicalWordCount(), new String[] {  INPUT, OUTPUT });
		
		assertEquals(6, assertOutput(OUTPUT + "/part-r-00000", new Configuration()));
	}
	
	public static int assertOutput(String output, Configuration conf) throws NumberFormatException, IOException, InterruptedException {
		int validatedOutputLines = 0;
				
		TupleInputReader reader = new TupleInputReader(conf);
		reader.initialize(new Path(output), conf);
		while(reader.nextKeyValueNoSync()) {
			ITuple tuple = reader.getCurrentKey();
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
	
	@After
	@Before
	public void cleanUp() throws IOException {
		Runtime.getRuntime().exec("rm " + INPUT);
		Runtime.getRuntime().exec("rm -rf " + OUTPUT);
	}
}
