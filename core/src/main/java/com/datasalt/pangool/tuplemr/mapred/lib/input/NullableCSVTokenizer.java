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
package com.datasalt.pangool.tuplemr.mapred.lib.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.googlecode.jcsv.CSVStrategy;
import com.googlecode.jcsv.reader.CSVTokenizer;

/**
 * A custom CSVTokenizer policy that detects and provides null values if needed.
 * Null values can happen in two ways: 1) If strict quoting mode is enabled, anything that is not quoted means null.
 * 2) If a null string is provided, if it is found without quotes then it is null.
 * <p>
 * The null string provided can be null which means there will be no null string at all.
 */
public class NullableCSVTokenizer implements CSVTokenizer {

	enum State {
		NORMAL, QUOTED, QUOTED_FINISHED
	}

	final private boolean strictQuotes;
	final private char escapeCharacter;
	final private String nullString;
	
	// 200 Mb maximun record size
	private int maxFieldSize = 1024*1024*200; 

	/**
	 * Escaping wasn't handled by the provided JSCV tokenizer so we add it here.
	 */
	public NullableCSVTokenizer(char escapeCharacter, boolean strictQuotes, String nullString) {
		this.strictQuotes = strictQuotes;
		this.escapeCharacter = escapeCharacter;
		this.nullString = nullString;
	}

	@Override
	public List<String> tokenizeLine(String line, CSVStrategy strategy, BufferedReader reader)
	    throws IOException {

		final char DELIMITER = strategy.getDelimiter();
		final char QUOTE = strategy.getQuoteCharacter();

		final boolean useQuotes = !(TupleTextInputFormat.NO_QUOTE_CHARACTER == QUOTE);
		final boolean useEscape = !(TupleTextInputFormat.NO_ESCAPE_CHARACTER == escapeCharacter);

		final char NEW_LINE = '\n';

		final StringBuilder sb = new StringBuilder(30);
		final List<String> token = new ArrayList<String>();

		line += NEW_LINE;
		State state = State.NORMAL;

		int pointer = 0;
		boolean lastValueQuoted = false;

		while(true) {
			final char c = line.charAt(pointer);

			// Check max record size
			if (sb.length() >= maxFieldSize) {
				throw new IOException("Field too long: " + sb.length() + " bytes. Did you close properly the quotes on records?");
			}
			
			// Escaping characters.
			if(c == escapeCharacter && useEscape) {
				pointer++;
				char next = line.charAt(pointer);
				if (next != NEW_LINE) {
					sb.append(next);
					pointer++;
				}
				continue;
			}
			
			switch(state) {
			case NORMAL:
				if(c == DELIMITER || c == NEW_LINE) {
					String str = sb.toString();
					if(useQuotes && !lastValueQuoted) {
						if(strictQuotes) {
							token.add(null);
						} else if (nullString != null && (nullString.equals(str) || ("".equals(nullString) && "".equals(str.trim()))) ) {
							token.add(null);
						} else {
							token.add(str);
						}
					} else {
						if (useQuotes && lastValueQuoted) {
							token.add(str);
						} else if (nullString != null && (nullString.equals(str) || ("".equals(nullString) && "".equals(str.trim()))) ) {
							token.add(null);
						} else {
							token.add(str);
						}
					}
					lastValueQuoted = false;
					sb.delete(0, sb.length());
					if(c == NEW_LINE) {
						return token;
					}
				} else if(c == QUOTE && useQuotes) {
					sb.delete(0, sb.length());
					state = State.QUOTED;
					lastValueQuoted = true;
				} else {
					sb.append(c);
				}
				break;

			case QUOTED:
				if(c == NEW_LINE && reader != null) {
					sb.append(NEW_LINE);
					pointer = -1;
					line = reader.readLine();
					if(line == null) {
						throw new IllegalStateException("unexpected end of file, unclosed quotation");
					}
					line += NEW_LINE;
				} else if(c == QUOTE) {
					state = State.QUOTED_FINISHED;
				} else {
					sb.append(c);
				}
				break;
				
			case QUOTED_FINISHED:
				// just skipping characters after the quotes
				if(c == DELIMITER || c == NEW_LINE) {
					state = State.NORMAL;
					continue;
				} 
				break;
			}			

			pointer++;
		}
	}

	public int getMaxFieldSize() {
		return maxFieldSize;
	}

	public void setMaxFieldSize(int maxRecordSize) {
		this.maxFieldSize = maxRecordSize;
	}		
}
