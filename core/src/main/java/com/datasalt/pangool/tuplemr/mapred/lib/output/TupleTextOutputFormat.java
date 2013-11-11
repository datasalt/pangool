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
import java.io.Serializable;

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
public class TupleTextOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements Serializable {

	public static final char NO_QUOTE_CHARACTER = CSVWriter.NO_QUOTE_CHARACTER;
	public static final char NO_ESCAPE_CHARACTER = CSVWriter.NO_ESCAPE_CHARACTER;

	private final Schema schema;
	private final char separatorCharacter;
	private final char quoteCharacter;
	private final char escapeCharacter;
	private final boolean addHeader;
	private final String nullString;
	
	public TupleTextOutputFormat(Schema schema, boolean addHeader, char separatorCharacter, char quoteCharacter,
	    char escapeCharacter) {
		this(schema, addHeader, separatorCharacter, quoteCharacter, escapeCharacter, null);
	}
	
	/**
	 * You must specify the Schema that will be used for Tuples being written and the CSV semantics (if any). Use
	 * {@link #NO_ESCAPE_CHARACTER} and {@link #NO_QUOTE_CHARACTER} if you don't want to add CSV semantics to the output.
	 * If addHeader is true, the name of the Fields in the Schema will be used to add a header to the file.
	 * <p>
	 * Use "nullString" to replace nulls with some string.
	 */
	public TupleTextOutputFormat(Schema schema, boolean addHeader, char separatorCharacter, char quoteCharacter,
	    char escapeCharacter, String nullString) {
		
		this.schema = schema;
		this.addHeader = addHeader;
		this.separatorCharacter = separatorCharacter;
		this.quoteCharacter = quoteCharacter;
		this.escapeCharacter = escapeCharacter;
		this.nullString = nullString;
	}

	@Override
	public RecordWriter<ITuple, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
	    InterruptedException {

		Path file = getDefaultWorkFile(context, "");
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(file.getFileSystem(context.getConfiguration())
		    .create(file)));
		CSVWriter csvWriter = new CSVWriter(writer, separatorCharacter, quoteCharacter, escapeCharacter);
		if(addHeader) {
			String[] header = new String[schema.getFields().size()];
			for(int i = 0; i < schema.getFields().size(); i++) {
				header[i] = schema.getFields().get(i).getName();
			}
			csvWriter.writeNext(header);
		}
		return new TupleTextRecordWriter(schema, csvWriter, nullString);
	}

	public static class TupleTextRecordWriter extends RecordWriter<ITuple, NullWritable> {

		private final CSVWriter writer;
		private final Schema schema;
		private final String[] lineToWrite;
		private final String nullString;

		public TupleTextRecordWriter(Schema schema, CSVWriter writer, String nullString) {
			this.writer = writer;
			this.schema = schema;
			int nFields = schema.getFields().size();
			lineToWrite = new String[nFields];
			this.nullString = nullString;
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
				throw new IOException("Input schema has different number of fields [" + tuple.getSchema().getFields().size()
				    + "] not matching output format Schema fields [" + schema.getFields().size() + "]");
			}
			// Convert the tuple to an array of Strings
			for(int i = 0; i < tuple.getSchema().getFields().size(); i++) {
				Object obj = tuple.get(i);
				if(obj != null) {
					lineToWrite[i] = obj.toString();
				} else if(nullString != null) {
					lineToWrite[i] = nullString;
				} else {
					throw new IOException("Null object in field (" + tuple.getSchema().getField(i).getName() + ") and no null string specified by constructor.");
				}
			}
			// Write it to the CSV writer
			writer.writeNext(lineToWrite);
		}
	}
}