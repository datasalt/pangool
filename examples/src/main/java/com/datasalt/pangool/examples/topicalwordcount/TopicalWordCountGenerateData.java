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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.examples.Utils;

/**
 * Input data generator for the {@link TopicalWordCount} and {@link TopicalWordCountWithStopWords} example.
 */
public class TopicalWordCountGenerateData {

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
			int randomTopic = (int) (Math.random() * nTopics);
			String text = Utils.randomString(2) + " " + Utils.randomString(3) + " " + Utils.randomString(4); 
			jsonRecord.put("topicId", randomTopic);
			jsonRecord.put("text", text);
			writer.write(mapper.writeValueAsString(jsonRecord) + "\n");
		}

		writer.close();
	}
}
