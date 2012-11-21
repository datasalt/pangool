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
	public void testQuotes() throws IOException {
		String []fields = getCSVParser(" \"\\\"Hello\\\"\" ,1\"hello\",\"hello\"1", ',', '"', '\\', false, null).readNext();
		assertEquals("\"Hello\"", fields[0]);
		assertEquals("hello", fields[1]);
		assertEquals("hello", fields[2]);
	}
	
	@Test
	public void testMultiLine() throws IOException {
		String []fields = getCSVParser("\"linea1\nlinea2\"", ',', '"', '\\', false, null).readNext();
		assertEquals("linea1\nlinea2", fields[0]);
	}	
	
	@Test
	public void testNulls() throws IOException {
		String[] fields = getCSVParser("\"Hello\",,\\N,3", ',', '"', '\\', false, "").readNext();
		assertEquals("Hello", fields[0]);
		assertEquals(null, fields[1]);
		assertEquals("N", fields[2]);
		assertEquals("3", fields[3]);
		
		// Strict quoting
		fields = getCSVParser("\"Hello\",,\\N,3", ',', '"', '\\', true, null).readNext();
		assertEquals("Hello", fields[0]);
		assertEquals(null, fields[1]);
		assertEquals(null, fields[2]);
		assertEquals(null, fields[3]);
		
		// Only \N means null
		fields = getCSVParser("\"Hello\",,\\\\N,3\\\"", ',', '"', '\\', false, "\\N").readNext();
		assertEquals("Hello", fields[0]);
		assertEquals("", fields[1]);
		assertEquals(null, fields[2]);
		assertEquals("3\"", fields[3]);
		
		// No quotes. Empty string means null
		fields = getCSVParser("3, ,", ',', TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER, false, "").readNext();
		assertEquals("3", fields[0]);
		assertEquals(null, fields[1]);
		assertEquals(null, fields[2]);
		
		// No quotes. One space string means null
		fields = getCSVParser("3, ,", ',', TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER, false, " ").readNext();
		assertEquals("3", fields[0]);
		assertEquals(null, fields[1]);
		assertEquals("", fields[2]);
		
		// Quotes. One space string means null
		fields = getCSVParser("\"3\",\" \",\"\", ,", ',', '\"', '\\', false, " ").
				readNext();
		assertEquals("3", fields[0]);
		assertEquals(" ", fields[1]);
		assertEquals("", fields[2]);	
		assertEquals(null, fields[3]);
		assertEquals("", fields[4]);
		

		// Quotes. One space string means null. Strict quotes
		fields = getCSVParser("\"3\",\" \",\"\", ,", ',', '\"', '\\', true, " ").
				readNext();
		assertEquals("3", fields[0]);
		assertEquals(" ", fields[1]);
		assertEquals("", fields[2]);	
		assertEquals(null, fields[3]);
		assertEquals(null, fields[4]);

	}

	@Test(expected=IOException.class)
	public void testMasRecordSize() throws IOException {
		NullableCSVTokenizer tok = new NullableCSVTokenizer('\\', false, null);
		tok.setMaxFieldSize(10);
		CSVReader<String[]> cvs = new CSVReaderBuilder<String[]>(new StringReader(
				"hola,que,\"tal va la vida en este mundo tan cruel\",te,va,yobien"))
		.strategy(new CSVStrategy(',', '"', '\\', false, true))
		.tokenizer(tok)
		.entryParser(new DefaultCSVEntryParser()).build();

		cvs.readNext();
	}

	
  public CSVReader<String[]> getCSVParser(String line, char separator, char quote, char escape, boolean strictQuotes, String nullString) {
		return
			new CSVReaderBuilder<String[]>(new StringReader(line))
				.strategy(new CSVStrategy(separator, quote, '#', false, true))
				.tokenizer(new NullableCSVTokenizer(escape, strictQuotes, nullString))
				.entryParser(new DefaultCSVEntryParser()).build();
	}
}
