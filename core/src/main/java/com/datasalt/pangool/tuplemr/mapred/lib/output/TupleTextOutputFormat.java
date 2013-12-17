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
package com.datasalt.pangool.tuplemr.mapred.lib.output;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.Writer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVWriter;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

/**
 * A special output format that supports converting a {@link ITuple} into text. It supports CSV-like semantics such as
 * separator character, quote character and escape character. It uses Open CSV underneath
 * (http://opencsv.sourceforge.net/).
 */
@SuppressWarnings("serial")
public class TupleTextOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements
    Serializable {

	public static final char NO_QUOTE_CHARACTER = CSVWriter.NO_QUOTE_CHARACTER;
	public static final char NO_ESCAPE_CHARACTER = CSVWriter.NO_ESCAPE_CHARACTER;

	private final Schema schema;
	private final char separatorCharacter;
	private final char quoteCharacter;
	private final char escapeCharacter;
	private final boolean addHeader;
	private final String nullString;

	public TupleTextOutputFormat(Schema schema, boolean addHeader, char separatorCharacter,
	    char quoteCharacter, char escapeCharacter) {
		this(schema, addHeader, separatorCharacter, quoteCharacter, escapeCharacter, null);
	}

	/**
	 * You must specify the Schema that will be used for Tuples being written and the CSV semantics (if any). Use
	 * {@link #NO_ESCAPE_CHARACTER} and {@link #NO_QUOTE_CHARACTER} if you don't want to add CSV semantics to the output.
	 * If addHeader is true, the name of the Fields in the Schema will be used to add a header to the file.
	 * <p>
	 * Use "nullString" to replace nulls with some string.
	 */
	public TupleTextOutputFormat(Schema schema, boolean addHeader, char separatorCharacter,
	    char quoteCharacter, char escapeCharacter, String nullString) {

		this.schema = schema;
		this.addHeader = addHeader;
		this.separatorCharacter = separatorCharacter;
		this.quoteCharacter = quoteCharacter;
		this.escapeCharacter = escapeCharacter;
		this.nullString = nullString;
	}

	@Override
	public RecordWriter<ITuple, NullWritable> getRecordWriter(TaskAttemptContext context)
	    throws IOException, InterruptedException {

		Path file = getDefaultWorkFile(context, "");
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(file.getFileSystem(
		    context.getConfiguration()).create(file)));
		CustomCSVWriter csvWriter = new CustomCSVWriter(writer, separatorCharacter, quoteCharacter,
		    escapeCharacter, nullString);
		if(addHeader) {
			String[] header = new String[schema.getFields().size()];
			for(int i = 0; i < schema.getFields().size(); i++) {
				header[i] = schema.getFields().get(i).getName();
			}
			csvWriter.writeNext(header);
		}
		return new TupleTextRecordWriter(schema, csvWriter);
	}

	/**
	 * We had to almost re-implement CSVWriter for properly supporting null strings. We can't reuse a lot of code due to
	 * inheritance / visibility problems.
	 */
	public static class CustomCSVWriter {

		String nullString;
		private Writer rawWriter;
		private PrintWriter pw;
		private char separator;
		private char quotechar;
		private char escapechar;

		public CustomCSVWriter(Writer writer, char separator, char quotechar, char escapechar,
		    String nullString) {
			this.rawWriter = writer;
			this.pw = new PrintWriter(writer);
			this.nullString = nullString;
			this.separator = separator;
			this.quotechar = quotechar;
			this.escapechar = escapechar;
		}

		public void writeNext(String[] toWrite) throws IOException {
			if(toWrite == null)
				return;

			StringBuilder sb = new StringBuilder(CSVWriter.INITIAL_STRING_SIZE);
			for(int i = 0; i < toWrite.length; i++) {

				if(i != 0) {
					sb.append(separator);
				}

				String nextElement = toWrite[i];
				if(nextElement == null) {
					if(nullString == null) {
						throw new IOException("Null field and no null string specified by constructor.");
					}
					sb.append(nullString);
				} else {
					if(quotechar != NO_QUOTE_CHARACTER)
						sb.append(quotechar);

					sb.append(stringContainsSpecialCharacters(nextElement) ? processLine(nextElement)
					    : nextElement);

					if(quotechar != NO_QUOTE_CHARACTER)
						sb.append(quotechar);
				}
			}

			sb.append(CSVWriter.DEFAULT_LINE_END);
			pw.write(sb.toString());
		}

		protected StringBuilder processLine(String nextElement) {
			StringBuilder sb = new StringBuilder(CSVWriter.INITIAL_STRING_SIZE);
			for(int j = 0; j < nextElement.length(); j++) {
				char nextChar = nextElement.charAt(j);
				if(escapechar != NO_ESCAPE_CHARACTER && nextChar == quotechar) {
					sb.append(escapechar).append(nextChar);
				} else if(escapechar != NO_ESCAPE_CHARACTER && nextChar == escapechar) {
					sb.append(escapechar).append(nextChar);
				} else {
					sb.append(nextChar);
				}
			}

			return sb;
		}

		private boolean stringContainsSpecialCharacters(String line) {
			return line.indexOf(quotechar) != -1 || line.indexOf(escapechar) != -1;
		}

		public void close() throws IOException {
			pw.flush();
			pw.close();
			rawWriter.close();
		}

	}

	public static class TupleTextRecordWriter extends RecordWriter<ITuple, NullWritable> {

		private final CustomCSVWriter writer;
		private final Schema schema;
		private final String[] lineToWrite;

		public TupleTextRecordWriter(Schema schema, CustomCSVWriter writer) {
			this.writer = writer;
			this.schema = schema;
			int nFields = schema.getFields().size();
			lineToWrite = new String[nFields];
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			writer.close();
		}

		@Override
		public void write(ITuple tuple, NullWritable toIgnore) throws IOException, InterruptedException {
			// Basic sanity checks
			if(!tuple.getSchema().getName().equals(schema.getName())) {
				throw new IOException("Mismatched schema name [" + tuple.getSchema().getName()
				    + "] does not match output format Schema [" + schema.getName() + "]");
			}
			if(tuple.getSchema().getFields().size() != schema.getFields().size()) {
				throw new IOException("Input schema has different number of fields ["
				    + tuple.getSchema().getFields().size() + "] not matching output format Schema fields ["
				    + schema.getFields().size() + "]");
			}
			// Convert the tuple to an array of Strings
			for(int i = 0; i < tuple.getSchema().getFields().size(); i++) {
				Object obj = tuple.get(i);
				if(obj != null) {
					lineToWrite[i] = obj.toString();
				} else {
					lineToWrite[i] = null;
				}
			}
			// Write it to the CSV writer
			writer.writeNext(lineToWrite);
		}
	}
}