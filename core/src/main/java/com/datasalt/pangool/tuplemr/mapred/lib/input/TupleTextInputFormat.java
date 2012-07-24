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
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;

/**
 * A special input format that supports reading text lines into {@link ITuple}. It supports CSV-like semantics such as
 * separator character, quote character and escape character. It uses Open CSV underneath
 * (http://opencsv.sourceforge.net/).
 */
@SuppressWarnings("serial")
public class TupleTextInputFormat extends FileInputFormat<ITuple, NullWritable> implements Serializable {

	public static final char NO_QUOTE_CHARACTER = CSVWriter.NO_QUOTE_CHARACTER;
	public static final char NO_ESCAPE_CHARACTER = CSVWriter.NO_ESCAPE_CHARACTER;

	private final Schema schema;
	private final boolean hasHeader;
	private final char separatorCharacter;
	private final char quoteCharacter;
	private final char escapeCharacter;

	/**
	 * You must specify the Schema that will be used for Tuples being read so that automatic type conversions can be
	 * applied (i.e. parsing) and the CSV semantics (if any). Use {@link #NO_ESCAPE_CHARACTER} and
	 * {@link #NO_QUOTE_CHARACTER} if the input files don't have any such semantics. If hasHeader is true, the first line
	 * of any file will be skipped.
	 */
	public TupleTextInputFormat(final Schema schema, boolean hasHeader, Character separator, Character quoteCharacter,
	    Character escapeCharacter) {
		this.schema = schema;
		for(Field field : schema.getFields()) {
			if(field.getType().equals(Type.OBJECT) || field.getType().equals(Type.BYTES)) {
				throw new IllegalArgumentException(this.getClass().getName() + " doesn't support Pangool types " + Type.OBJECT
				    + " or " + Type.BYTES);
			}
		}
		this.hasHeader = hasHeader;
		this.separatorCharacter = separator;
		this.quoteCharacter = quoteCharacter;
		this.escapeCharacter = escapeCharacter;
	}

	public static class TupleTextInputReader extends RecordReader<ITuple, NullWritable> {

		private CSVReader reader;
		private final Character separator;
		private final Character quote;
		private final Character escape;
		private final boolean hasHeader;

		private long start = 0;
		private long end = Integer.MAX_VALUE;
		private long position = 0;

		private final Schema schema;
		private final ITuple tuple;

		public TupleTextInputReader(Schema schema, boolean hasHeader, Character separator, Character quote, Character escape) {
			this.separator = separator;
			this.quote = quote;
			this.escape = escape;
			this.schema = schema;
			this.hasHeader = hasHeader;
			this.tuple = new Tuple(schema);
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}

		@Override
		public ITuple getCurrentKey() throws IOException, InterruptedException {
			return tuple;
		}

		@Override
		public NullWritable getCurrentValue() throws IOException, InterruptedException {
			return NullWritable.get();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if(end == start) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (position - start) / (float) (end - start));
			}
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) split;
			Path pathToRead = fileSplit.getPath();
			init(pathToRead, context.getConfiguration());
			this.start = fileSplit.getStart();
			this.end = fileSplit.getStart() + split.getLength();
		}
		
		public void init(Path pathToRead, Configuration conf) throws IOException {
			FileSystem fS = pathToRead.getFileSystem(conf);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fS.open(pathToRead)));
			// Skip start lines?
			reader.skip(start);
			if(start == 0) {
				// Watch out possible header line!
				if(hasHeader) {
					reader.skip(1);
				}
			}
			this.reader = new CSVReader(reader, separator, quote, escape);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			String[] readLine = reader.readNext();
			if(readLine == null) {
				return false;
			}
			if(readLine.length != schema.getFields().size()) {
				throw new IOException("Read line [" + Arrays.toString(readLine) + "] with [" + readLine.length
				    + "] fields which is less than specified Schema [" + schema + "] fields [" + schema.getFields().size()
				    + "]");
			}
			for(int i = 0; i < schema.getFields().size(); i++) {
				Field field = schema.getFields().get(i);
				try {
					switch(field.getType()) {
					case DOUBLE:
						tuple.set(i, Double.parseDouble(readLine[i]));
						break;
					case FLOAT:
						tuple.set(i, Float.parseFloat(readLine[i]));
						break;
					case ENUM:
						Class clazz = field.getObjectClass();
						tuple.set(i, Enum.valueOf(clazz, readLine[i]));
						break;
					case INT:
						tuple.set(i, Integer.parseInt(readLine[i]));
						break;
					case LONG:
						tuple.set(i, Long.parseLong(readLine[i]));
						break;
					case STRING:
						tuple.set(i, readLine[i]);
						break;
					case BOOLEAN:
						tuple.set(i, Boolean.parseBoolean(readLine[i]));
						break;
					}
				} catch(Throwable t) {
					t.printStackTrace();
					throw new IOException("Data mismatch reading line [" + Arrays.toString(readLine)
					    + "] when trying to parse field [" + i + "] of schema [" + schema + "]", t);
				}
			}
			return true;
		}
	}

	public Schema getSchema() {
		return schema;
	}

	public boolean isHasHeader() {
		return hasHeader;
	}

	public char getSeparatorCharacter() {
		return separatorCharacter;
	}

	public char getQuoteCharacter() {
		return quoteCharacter;
	}

	public char getEscapeCharacter() {
		return escapeCharacter;
	}

	@Override
	public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit iS, TaskAttemptContext context)
	    throws IOException, InterruptedException {
		return new TupleTextInputReader(schema, hasHeader, separatorCharacter, quoteCharacter, escapeCharacter);
	}
}
