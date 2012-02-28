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
package com.datasalt.pangool.benchmark.wordcount;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import com.datasalt.pangool.benchmark.secondarysort.CascadingSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.CrunchSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.HadoopSecondarySort;

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
			    .println("Usage: [out-file-name] [#number_of_lines] [#number_of_words_per_line] [#number_of_chars_per_word]");
			System.err.println();
			System.err
			    .println("Example: foo.txt 10 10 2 -> Will generate a foo.txt file with 10x10 = 100 words of 2 chars each, 10 in each line.");
			System.err.println();
			System.exit(-1);
		}

		BufferedWriter writer = new BufferedWriter(new FileWriter(args[0]));

		final int nLines = Integer.parseInt(args[1]);
		final int nWordsPerLine = Integer.parseInt(args[2]);
		final int nChars = Integer.parseInt(args[3]);
		
		for(int i = 0; i < nLines; i++) {
			for(int j = 0; j < nWordsPerLine; j++) {
				writer.write(randomWord(nChars) + " ");
			}
			writer.write("\n");
		}
		writer.close();
	}

	public static String randomWord(int nChars) {
		String word = "";
		for(int i = 0; i < nChars; i++) {
			word += randomChar();
		}
		return word;
	}
	
	public static char randomChar() {
		return (char) ((int) (Math.random() * 26) + 'a');
	}
}