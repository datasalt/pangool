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

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapreduce.lib.output.AvroOutputFormat;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.xerial.snappy.SnappyCodec;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.utils.AvroUtils;
import com.datasalt.pangool.utils.TupleToAvroRecordConverter;

/**
 * An Avro-based output format for {@link ITuple}s
 */
@SuppressWarnings("serial")
public class TupleOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements Serializable {

	public final static String FILE_PREFIX = "tuple";

	public static final String DEFLATE_CODEC = "deflate";
	public static final String SNAPPY_CODEC = "snappy";

	private static final int SYNC_SIZE = 16;
	private static final int DEFAULT_SYNC_INTERVAL = 1000 * SYNC_SIZE;

	private String pangoolOutputSchema;

	public TupleOutputFormat(String pangoolOutputSchema) {
		this.pangoolOutputSchema = pangoolOutputSchema;
	}

	public static class TupleRecordWriter extends RecordWriter<ITuple, NullWritable> {

		protected Record record;
		protected DataFileWriter<Record> writer;
		private TupleToAvroRecordConverter converter;

		public TupleRecordWriter(Schema pangoolSchema, DataFileWriter<Record> writer, Configuration conf) {
			this.writer = writer;
			converter = new TupleToAvroRecordConverter(pangoolSchema, conf);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			writer.close();
		}

		@Override
		public void write(ITuple tuple, NullWritable ignore) throws IOException, InterruptedException {
			record = converter.toRecord(tuple, record);
			writer.append(record);
		}
	}

	@Override
	public RecordWriter<ITuple, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
	    InterruptedException {

		Schema pangoolOutputSchema = Schema.parse(this.pangoolOutputSchema);
		org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(pangoolOutputSchema);
		DataFileWriter<Record> writer = new DataFileWriter<Record>(new ReflectDatumWriter<Record>());

		// Compression etc - use Avro codecs
		Configuration conf = context.getConfiguration();
		if(conf.getBoolean("mapred.output.compress", false)) {
			String codec = conf.get("mapred.output.compression.codec");
			if(codec.equals(SnappyCodec.class.getName())) {
				codec = SNAPPY_CODEC;
			} else {
				codec = "null";
			}
			int level = conf.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, AvroOutputFormat.DEFAULT_DEFLATE_LEVEL);
			CodecFactory factory = codec.equals(DEFLATE_CODEC) ? CodecFactory.deflateCodec(level) : CodecFactory
			    .fromString(codec);
			writer.setCodec(factory);
		}
		writer.setSyncInterval(conf.getInt(AvroOutputFormat.SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL));

		Path file = getDefaultWorkFile(context, "");
		writer.create(avroSchema, file.getFileSystem(context.getConfiguration()).create(file));

		return new TupleRecordWriter(pangoolOutputSchema, writer, conf);
	}
}
