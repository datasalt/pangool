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
 */
public class NullableCSVTokenizer implements CSVTokenizer {

	enum State {
		NORMAL, QUOTED
	}

	final private boolean strictQuotes;
	final private char escapeCharacter;
	final private String nullString;

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

			switch(state) {
			case NORMAL:
				if(c == DELIMITER || c == NEW_LINE) {
					String str = sb.toString();
					if(useQuotes && !lastValueQuoted) {
						if(strictQuotes) {
							token.add(null);
						} else if(nullString != null && str.equals(nullString)) {
							token.add(null);
						} else {
							token.add(str);
						}
					} else {
						token.add(str);
					}
					lastValueQuoted = false;
					sb.delete(0, sb.length());
					if(c == NEW_LINE) {
						return token;
					}
				} else if(c == QUOTE && useQuotes) {
					if(sb.length() == 0) {
						state = State.QUOTED;
						lastValueQuoted = true;
					} else if(line.charAt(pointer + 1) == QUOTE && sb.length() > 0) {
						sb.append(c);
						pointer++;
					} else if(line.charAt(pointer + 1) != QUOTE) {
						state = State.QUOTED;
						lastValueQuoted = true;
					}
				} else {
					sb.append(c);
				}
				break;

			case QUOTED:
				if(c == escapeCharacter && useEscape) {
					pointer++;
					sb.append(line.charAt(pointer));
					break;
				} else if(c == NEW_LINE && reader != null) {
					sb.append(NEW_LINE);
					pointer = -1;
					line = reader.readLine();
					if(line == null) {
						throw new IllegalStateException("unexpected end of file, unclosed quotation");
					}
					line += NEW_LINE;
				} else if(c == QUOTE) {
					state = State.NORMAL;
				} else {
					sb.append(c);
				}
				break;
			}

			pointer++;
		}
	}
}
