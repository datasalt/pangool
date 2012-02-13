package com.datasalt.pangool.benchmark.secondarysort;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This program generates input that can be used for running {@link PangoolSecondarySort}, {@link HadoopSecondarySort},
 * {@link CascadingSecondarySort}, {@link CrunchSecondarySort}
 * <p>
 * The generated output will a tabulated text file with the form: {department idPerson timestamp sale}
 * <p>
 */
public class GenerateData {

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

		for(int i = 0; i < nDeps; i++) {
			int randomDep = (int) (Math.random() * INTRANGE);
			for(int j = 0; j < nPersonPerDep; j++) {
				String randomName = "" + randomChar() + randomChar();
				for(int k = 0; k < nPaymentsPerPerson; k++) {
					long randomDate = System.currentTimeMillis() - (int) (Math.random() * TIMEFRAME);
					double randomPrice = Math.random() * INTRANGE;
					writer.write(randomDep + "\t" + randomName + "\t" + randomDate + "\t" + randomPrice + "\n");
				}
			}
		}
		writer.close();
	}

	public static char randomChar() {
		return (char) ((int) (Math.random() * 26) + 'a');
	}
}
