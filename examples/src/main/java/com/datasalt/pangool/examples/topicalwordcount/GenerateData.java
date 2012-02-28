package com.datasalt.pangool.examples.topicalwordcount;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.examples.TestUtils;

public class GenerateData {

	public final static void main(String[] args) throws IOException {
		if(args.length != 3) {
			System.err.println();
			System.err.println("Three arguments are needed.");
			System.err.println("Usage: [out-file] [nRegisters] [nTopics]");
			System.err.println();
			System.err
			    .println("Example: texts.txt 10 2 -> Will generate a JSON file 'texts.txt' with 10 registers from 2 different topics.");
			System.err.println();
			System.exit(-1);
		}
		
		String outFile = args[0];
		int nRegisters = Integer.parseInt(args[1]);
		int nTopics = Integer.parseInt(args[2]);

		ObjectMapper mapper = new ObjectMapper();
		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		Map<String, Object> jsonRecord = new HashMap<String, Object>();
		for(int i = 0; i < nRegisters; i++) {
			int randomTopic = (int) (Math.random() % nTopics);
			String text = TestUtils.randomString(10) + " " + TestUtils.randomString(10) + " " + TestUtils.randomString(10); 
			jsonRecord.put("topic", randomTopic);
			jsonRecord.put("text", text);
			writer.write(mapper.writeValueAsString(jsonRecord) + "\n");
		}

		writer.close();
	}
}
