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
 * A custom CSVTokenizer for input lines with fixed width fields.  
 * <p>
 * If a null string is provided then it is null when found in a field.
 * <p>
 * The null string provided can be null which means there will be no null string at all.
 */
public class FixedWidthCSVTokenizer implements CSVTokenizer {

	final private String nullString;
	final private int fields[];

	/**
	 * Fixed width tokeninzer
   * <ul>
	 *   <li><strong>fields</strong>: must contains pairs of positions, indicating the location of fields in the line.
	 *   The positions must be pairs of starPosition and endPosition, starting on 0, so that the substring
	 *   including the characters at startPosition and endPosition are provided.</li>
	 *   <li><strong>nullString</strong>: considering a field null when found. No effect if null.
	 *   The field is trimmed before performed the comparison.</li>
	 * </ul>  
	 */
	public FixedWidthCSVTokenizer(int []fields, String nullString) {
		this.fields = fields;
		this.nullString = (nullString != null) ? nullString.trim() : null;
		
		if (fields.length%2 != 0) {
			throw new IllegalArgumentException("Illegal fields parameter size [" + fields.length +"]. Expecting an even size.");
		}
		for (int i = 0; i<fields.length; i += 2) {
			int min = fields[i];
			int max = fields[i+1];
			if ( max < min || min < 0) {
				throw new IllegalArgumentException("Incorrect field range [" + min + "," + max + "]. max position cannot be smaller than min position and both must be positive.");
			}
		}		
	}

	@Override
	public List<String> tokenizeLine(String line, CSVStrategy strategy, BufferedReader reader)
	    throws IOException {

		final List<String> tokens = new ArrayList<String>();

		for (int i = 0; i<fields.length; i += 2) {
			int min = fields[i];
			int max = fields[i+1];
			if (Math.max(min,max) >= line.length()) {
				throw new IOException("Field delimited by positions [" + min + "," + max + "] out of range for line [" + line + "] of size [" + line.length() + "]");
			}
			String token = line.substring(min, max+1); 
			if (nullString != null && nullString.equals(token.trim())) {
				tokens.add(null);
			} else {
				tokens.add(token);
			}
		}
		return tokens;
	}
}
