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
package com.datasalt.pangool.examples.useractivitynormalizer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Input data generator for the {@link UserActivityNormalizer} example.
 */
public class UserActivityNormalizerGenerateData {

	public final static void main(String[] args) throws IOException {
		if(args.length != 4) {
			System.err.println();
			System.err.println("Three arguments are needed.");
			System.err.println("Usage: [out-file] [nRegisters] [nUsers] [nFeatures]");
			System.err.println();
			System.err
			    .println("Example: user-features.txt 10 5 2 -> Will generate a file 'user-features.txt' with 10 registers having 5 different users interacting with 2 different features.");
			System.err.println();
			System.exit(-1);
		}

		String outFile = args[0];
		int nRegisters = Integer.parseInt(args[1]);
		int nFeatures = Integer.parseInt(args[2]);
		int nUsers = Integer.parseInt(args[3]);

		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		for(int i = 0; i < nRegisters; i++) {
			int randomFeature = (int) (Math.random() * nFeatures) + 1;
			int randomUser    = (int) (Math.random() * nUsers) + 1;
			int randomClicks  = (int) (Math.random() * 10000) + 1;
			writer.write("user" + randomUser + "\t" + "feature" + randomFeature + "\t" + randomClicks + "\n");
		}

		writer.close();
	}
}
