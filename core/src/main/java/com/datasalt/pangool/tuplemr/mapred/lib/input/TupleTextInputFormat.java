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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import au.com.bytecode.opencsv.CSVWriter;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.googlecode.jcsv.CSVStrategy;
import com.googlecode.jcsv.reader.CSVTokenizer;

/**
 * A special input format that supports reading text lines into {@link ITuple}. It supports CSV-like semantics such as
 * separator character, quote character and escape character. It uses Open CSV underneath
 * (http://opencsv.sourceforge.net/).
 */
@SuppressWarnings("serial")
public class TupleTextInputFormat extends FileInputFormat<ITuple, NullWritable> implements Serializable {

	public static final char NO_QUOTE_CHARACTER = CSVWriter.NO_QUOTE_CHARACTER;
	public static final char NO_ESCAPE_CHARACTER = CSVWriter.NO_ESCAPE_CHARACTER;
	public static final char NO_SEPARATOR_CHARACTER = '\u0000';
	public static final String NO_NULL_STRING = null;

	private final InputType type;
	private final Schema schema;
	private final boolean hasHeader;
	private final boolean strictQuotes;
	private final char separatorCharacter;
	private final char quoteCharacter;
	private final char escapeCharacter;
	private FieldSelector fieldSelector = null;
	private String nullString;
	// To be used only when type is FIXED_WIDTH. Pairs of positions per each field.
	private int []fixedWidthFieldsPositions = null;

	private enum InputType {CSV, FIXED_WIDTH};
	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	/**
	 * When provided, will use it as a mapping between the text file columns and the provided Schema. This is useful if
	 * the text file has a lot of columns but we only care about some of them. For example if we provide a 3-field Schema
	 * we then could provide 3 indexes here that will map to the schema by column index.
	 * <p>
	 * Remember that indexes go from [0 to n - 1]
	 */
	public static class FieldSelector implements Serializable {

		private Integer[] fieldIndexesToSelect;

		public FieldSelector(Integer... fieldIndexesToSelect) {
			this.fieldIndexesToSelect = fieldIndexesToSelect;
		}

		public int select(int index) {
			if(fieldIndexesToSelect.length > 0) {
				return fieldIndexesToSelect[index];
			}
			return index;
		}

		// Use this for bypassing Field selection
		public static final FieldSelector NONE = new FieldSelector();
	}
	
	/**
	 * Character separated files reader. 
	 * You must specify the Schema that will be used for Tuples being read so that automatic type conversions can be
	 * applied (i.e. parsing) and the CSV semantics (if any). Use {@link #NO_ESCAPE_CHARACTER} and
	 * {@link #NO_QUOTE_CHARACTER} if the input files don't have any such semantics. If hasHeader is true, the first line
	 * of any file will be skipped.
	 * <p>
	 * Additional options for parsing the input file are:
	 * <ul>
	 *   <li>A {@link FieldSelector} in case only a subset of the columns are needed.</li>
	 *   <li>A "strict quotes" flag which means any value which is not quoted will be returned as null.</li>
	 *   <li>A "null string" text which will be substituted by null if found. For example, the sequence "\N" used by MySQL dumps. The null string
	 *   can be null itself which means this feature is disabled.</li>
	 * </ul> 
	 */
	public TupleTextInputFormat(final Schema schema, boolean hasHeader, boolean strictQuotes,
	    Character separator, Character quoteCharacter, Character escapeCharacter,
	    FieldSelector fieldSelector, String nullString) {
		type = InputType.CSV;
		checkSchema(schema);
		this.schema = schema;		
		this.strictQuotes = strictQuotes;
		this.hasHeader = hasHeader;
		this.separatorCharacter = separator;
		this.quoteCharacter = quoteCharacter;
		this.escapeCharacter = escapeCharacter;
		this.fieldSelector = fieldSelector;
		this.nullString = nullString;
	}

	/**
	 * Fixed width fields file reader. 
	 * You must specify the Schema that will be used for Tuples being read so that automatic type conversions can be
	 * applied (i.e. parsing). If hasHeader is true, the first line
	 * of any file will be skipped.
   * <ul>
	 *   <li><strong>fields</strong>: must contains pairs of positions, indicating the location of fields in the line.
	 *   The array must contains pairs of starPosition and endPosition, so that the substring
	 *   including the characters at startPosition and endPosition is a field. The first characters is at
	 *   position 0 of the line.</li>
	 *   <li><strong>nullString</strong>: considering a field null when found. No effect if null. 
	 *   The field is trimmed before performed the comparison.</li>
	 * </ul>   
	 */
	public TupleTextInputFormat(Schema schema, int[] fields, boolean hasHeader, String nullString) {
		type = InputType.FIXED_WIDTH;
		checkSchema(schema);
		this.schema = schema;
		this.strictQuotes = false;
		this.hasHeader = hasHeader;
		this.separatorCharacter = NO_SEPARATOR_CHARACTER;
		this.quoteCharacter = NO_QUOTE_CHARACTER;
		this.escapeCharacter = NO_ESCAPE_CHARACTER;		
		this.nullString = nullString;
		checkFixedWithFields(schema, fields);
		this.fixedWidthFieldsPositions = fields;		
	}
	
	private void checkFixedWithFields(Schema schema, int[] fields) {
		if (schema.getFields().size()*2 != fields.length) {
			throw new IllegalArgumentException("Array with field positions for fixed width fields of incorrect size [" 
					+ fields.length + "]. Expected size [" + schema.getFields().size()*2 +"] for the schema [" + schema + "] of size [" 
					+ schema.getFields().size() +"]");
		}		
		for (int i=0; i<fields.length; i+=2) {
			int min = fields[i];
			int max = fields[i+1];
			if ( max < min ) {
				throw new IllegalArgumentException("Incorrect field range [" + min + "," + max + "]. max position cannot be smaller than min position."); 
			}
		}
	}
	
	private void checkSchema(Schema schema) {
		for(Field field : schema.getFields()) {
			if(field.getType().equals(Type.OBJECT) || field.getType().equals(Type.BYTES)) {
				throw new IllegalArgumentException(this.getClass().getName() + " doesn't support Pangool types "
				    + Type.OBJECT + " or " + Type.BYTES);
			}
		}		
	}

	public static class TupleTextInputReader extends RecordReader<ITuple, NullWritable> {

		private static final Log LOG = LogFactory.getLog(TupleTextInputReader.class);

		private CompressionCodecFactory compressionCodecs = null;

		private CSVTokenizer csvTokenizer;
		private CSVStrategy csvStrategy;
		private final Character separator;
		private final Character quote;
		private final boolean hasHeader;
		private final FieldSelector fieldSelector;
		private Text line;

		private LineReader in;
		private int maxLineLength;

		private long start = 0;
		private long end = Integer.MAX_VALUE;
		private long position = 0;

		private final Schema schema;
		private ITuple tuple;

		public TupleTextInputReader(Schema schema, boolean hasHeader, boolean strictQuotes,
		    Character separator, Character quote, Character escape, FieldSelector fieldSelector,
		    String nullString) {
			this.separator = separator;
			this.quote = quote;
			this.schema = schema;
			this.hasHeader = hasHeader;
			this.fieldSelector = fieldSelector;
			csvTokenizer = new NullableCSVTokenizer(escape, strictQuotes, nullString);
		}
		
		public TupleTextInputReader(Schema schema, int[] fields, boolean hasHeader, String nullString) {
			this.separator = NO_SEPARATOR_CHARACTER;
			this.quote = NO_QUOTE_CHARACTER;
			this.schema = schema;
			this.hasHeader = hasHeader;
			this.fieldSelector = null;
			csvTokenizer = new FixedWidthCSVTokenizer(fields, nullString);
		}
		
		@Override
		public void close() throws IOException {
			if(in != null) {
				in.close();
			}
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

		public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration conf = context.getConfiguration();
			this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			
			boolean skipFirstLine = false;
			csvStrategy = new CSVStrategy(separator, quote, '#', hasHeader && start == 0, true);

			// Skip txt header
			skipFirstLine = (hasHeader && start == 0);
			
			start = split.getStart();
			end = start + split.getLength();
			
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(conf);
			final CompressionCodec codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream fileIn = fs.open(split.getPath());
			if(codec != null) {
				in = new LineReader(codec.createInputStream(fileIn), conf);
				end = Long.MAX_VALUE;
			} else {
				if(start != 0) {
					// Skipping first line because we are not the first split, so start could not be
					// the start of the record
					skipFirstLine = true;
					--start;
					fileIn.seek(start);
				}
				in = new LineReader(fileIn, conf);
			}
			if (skipFirstLine) {
				// skip the line and re-establish "start".
				start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
			}
			this.position = start;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public boolean nextKeyValue() throws IOException {
			int newSize = 0;
			if(line == null) {
				this.line = new Text();
			}
			if(tuple == null) {
				this.tuple = new Tuple(schema);
			}
			while(position < end) {
				newSize = in.readLine(line, maxLineLength,
				    Math.max((int) Math.min(Integer.MAX_VALUE, end - position), maxLineLength));

				if(newSize < maxLineLength && newSize > 0) {
					List<String> readLine = csvTokenizer.tokenizeLine(line.toString(), csvStrategy, null);

					for(int i = 0; i < schema.getFields().size(); i++) {
						int index = i;
						if(fieldSelector != null) {
							index = fieldSelector.select(i);
						}
						String currentValue = "";
						try {
							currentValue = readLine.get(index);
							if(currentValue != null) {
								Field field = schema.getFields().get(i);
								switch(field.getType()) {
								case DOUBLE:
									tuple.set(i, Double.parseDouble(currentValue));
									break;
								case FLOAT:
									tuple.set(i, Float.parseFloat(currentValue));
									break;
								case ENUM:
									Class clazz = field.getObjectClass();
									tuple.set(i, Enum.valueOf(clazz, currentValue.trim()));
									break;
								case INT:
									tuple.set(i, Integer.parseInt(currentValue));
									break;
								case LONG:
									tuple.set(i, Long.parseLong(currentValue));
									break;
								case STRING:
									tuple.set(i, currentValue);
									break;
								case BOOLEAN:
									tuple.set(i, Boolean.parseBoolean(currentValue.trim()));
									break;
								default:
									throw new RuntimeException("Impossible case. This is a Bug.");
								}
							} else {
								tuple.set(i, null);
							}
						} catch(Throwable t) {
							LOG.warn("Error parsing value: (" + currentValue + ") in text line: (" + readLine + ")", t);
							// On any failure we assume null
							// The user is responsible for handling nulls afterwards
							tuple.set(i, null);
						}
					}
				}

				if(newSize == 0) {
					break;
				}
				position += newSize;
				if(newSize < maxLineLength) {
					break;
				}

				// line too long. try again
				LOG.info("Skipped line of size " + newSize + " at pos " + (position - newSize));
			}
			if(newSize == 0) {
				line = null;
				tuple = null;
				return false;
			} else {
				return true;
			}
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
	
	public InputType getType() {
		return type;
	}

	public boolean isStrictQuotes() {
		return strictQuotes;
	}

	public String getNullString() {
		return nullString;
	}

	public int[] getFixedWidthFieldsPositions() {
		return fixedWidthFieldsPositions;
	}

	@Override
	public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit iS, TaskAttemptContext context)
	    throws IOException, InterruptedException {
		if (type == InputType.CSV) {
				return new TupleTextInputReader(schema, hasHeader, strictQuotes, separatorCharacter, quoteCharacter,
						escapeCharacter, fieldSelector, nullString);
		} else {
				return new TupleTextInputReader(schema, fixedWidthFieldsPositions, hasHeader, nullString);
		}
	}
}
