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
package com.datasalt.pangool.examples.secondarysort;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import com.datasalt.pangool.examples.Utils;

/**
 * Input data generator for the {@link SecondarySort} example.
 */
public class SecondarySortGenerateData {

	final static int INTRANGE = 1000;
	final static int TIMEFRAME = 100000;

	public static void main(String[] args) throws IOException {
		if(args.length != 4) {
			System.err.println();
			System.err.println("Four arguments are needed.");
			System.err
			    .println("Usage: [out-file-name] [#number_of_departments] [#number_of_people_per_department] [#number_of_sales_per_people].");
			System.err.println();
			System.err
			    .println("Example: foo.txt 3, 3, 5 -> Will generate a foo.txt file with 3x3x5 = 45 records out of 3 departments with 3 people each and 5 sales actions for each of them.");
			System.err.println();
			System.exit(-1);
		}

		BufferedWriter writer = new BufferedWriter(new FileWriter(args[0]));

		final int nDeps = Integer.parseInt(args[1]);
		final int nPersonPerDep = Integer.parseInt(args[2]);
		final int nPaymentsPerPerson = Integer.parseInt(args[3]);
		Random r = new Random();
		for(int i = 0; i < nDeps; i++) {
			int randomDep = (r.nextInt());
			for(int j = 0; j < nPersonPerDep; j++) {
				String randomName = "" + Utils.randomChar() + Utils.randomChar();
				for(int k = 0; k < nPaymentsPerPerson; k++) {
					long randomDate = r.nextLong();
					double randomPrice = r.nextDouble();
					writer.write(randomDep + "\t" + randomName + "\t" + randomDate + "\t" + randomPrice + "\n");
				}
			}
		}
		writer.close();
	}
}
