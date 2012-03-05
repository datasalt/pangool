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
package com.datasalt.pangool.examples.simplesecondarysort;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SimpleSecondarySortGenerateData {

	public final static void main(String[] args) throws IOException {
		if(args.length != 3) {
			System.err.println();
			System.err.println("Three arguments are needed.");
			System.err.println("Usage: [out-file] [nRegisters] [maxIntValue]");
			System.err.println();
			System.err
			    .println("Example: numbers.txt 10 5 -> Will generate a file 'numbers.txt' with 10 pairs of numbers and each of them will be between [1, 5]");
			System.err.println();
			System.exit(-1);
		}
		
		String outFile = args[0];
		int nRegisters = Integer.parseInt(args[1]);
		int maxIntValue = Integer.parseInt(args[2]);

		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		for(int i = 0; i < nRegisters; i++) {
			int randomNumber1 = (int) (Math.random() * maxIntValue) + 1;
			int randomNumber2 = (int) (Math.random() * maxIntValue) + 1;
			writer.write(randomNumber1 + " " + randomNumber2 + "\n");
		}

		writer.close();
	}
}
