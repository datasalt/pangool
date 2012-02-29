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
package com.datasalt.pangool.examples.movingaverage;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Input data generator for the {@link MovingAverage} example.
 */
public class MovingAverageGenerateData {

	public final static void main(String[] args) throws IOException {
		if(args.length != 3) {
			System.err.println();
			System.err.println("Three arguments are needed.");
			System.err.println("Usage: [out-file] [nRegisters] [nUrls]");
			System.err.println();
			System.err
			    .println("Example: url_regs.txt 10 2 -> Will generate a file 'url_regs.txt' with 10 registers from 2 different urls.");
			System.err.println();
			System.exit(-1);
		}

		String outFile = args[0];
		int nRegisters = Integer.parseInt(args[1]);
		int nUrls = Integer.parseInt(args[2]);

		DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd");
		long now = System.currentTimeMillis();

		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		for(int i = 0; i < nRegisters; i++) {
			int urlId = (int) (Math.random() * nUrls);
			int randomCount = (int) (Math.random() * 10000);
			writer.write("url" + urlId + "\t" + format.print(now) + "\t" + randomCount + "\n");
			now -= 1000 * 60 * 60 * 24;
		}

		writer.close();
	}
}
