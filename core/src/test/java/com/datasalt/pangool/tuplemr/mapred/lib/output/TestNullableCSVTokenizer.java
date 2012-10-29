package com.datasalt.pangool.tuplemr.mapred.lib.output;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringReader;

import org.junit.Test;

import com.datasalt.pangool.tuplemr.mapred.lib.input.NullableCSVTokenizer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.googlecode.jcsv.CSVStrategy;
import com.googlecode.jcsv.reader.CSVReader;
import com.googlecode.jcsv.reader.internal.CSVReaderBuilder;
import com.googlecode.jcsv.reader.internal.DefaultCSVEntryParser;

public class TestNullableCSVTokenizer {

	@Test
	public void test() throws IOException {
		String[] fields = getCSVParser("Hello,1,2,", ',', TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER, false, null).readNext();
		assertEquals("Hello", fields[0]);
		assertEquals("1", fields[1]);
		assertEquals("2", fields[2]);
		assertEquals("", fields[3]);
		
		fields = getCSVParser("\"Hello\",1,\"2\",3", ',', '"', TupleTextInputFormat.NO_ESCAPE_CHARACTER, false, null).readNext();
		assertEquals("Hello", fields[0]);
		assertEquals("1", fields[1]);
		assertEquals("2", fields[2]);
		assertEquals("3", fields[3]);

		fields = getCSVParser("\"\\\"Hello\\\"\",1,\"2\",3", ',', '"', '\\', false, null).readNext();
		assertEquals("\"Hello\"", fields[0]);
		assertEquals("1", fields[1]);
		assertEquals("2", fields[2]);
		assertEquals("3", fields[3]);
	}

	@Test
	public void testNulls() throws IOException {
		String[] fields = getCSVParser("\"Hello\",,\\N,3", ',', '"', '\\', false, "").readNext();
		assertEquals("Hello", fields[0]);
		assertEquals(null, fields[1]);
		assertEquals("\\N", fields[2]);
		assertEquals("3", fields[3]);
		
		// Strict quoting
		fields = getCSVParser("\"Hello\",,\\N,3", ',', '"', '\\', true, null).readNext();
		assertEquals("Hello", fields[0]);
		assertEquals(null, fields[1]);
		assertEquals(null, fields[2]);
		assertEquals(null, fields[3]);
		
		// Only \N means null
		fields = getCSVParser("\"Hello\",,\\N,3", ',', '"', '\\', false, "\\N").readNext();
		assertEquals("Hello", fields[0]);
		assertEquals("", fields[1]);
		assertEquals(null, fields[2]);
		assertEquals("3", fields[3]);		
	}
	
  public CSVReader<String[]> getCSVParser(String line, char separator, char quote, char escape, boolean strictQuotes, String nullString) {
		return
			new CSVReaderBuilder<String[]>(new StringReader(line))
				.strategy(new CSVStrategy(separator, quote, '#', false, true))
				.tokenizer(new NullableCSVTokenizer(escape, strictQuotes, nullString))
				.entryParser(new DefaultCSVEntryParser()).build();
	}
}
