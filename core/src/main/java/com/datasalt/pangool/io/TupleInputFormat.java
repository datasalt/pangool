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
package com.datasalt.pangool.io;

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.serialization.hadoop.HadoopSerialization;
import com.datasalt.pangool.utils.AvroUtils;

@SuppressWarnings("serial")
public class TupleInputFormat extends FileInputFormat<ITuple, NullWritable> implements Serializable {

	public static class TupleInputReader extends RecordReader<ITuple, NullWritable> {

		private SpecificDatumReader<Record> specificReader;
		private FileReader<Record> reader;
		private long start;
		private long end;
		private HadoopSerialization ser;
		private Configuration conf;

		Tuple tuple;
		AvroWrapper<Record> wrapper;

		public TupleInputReader(Configuration conf) throws IOException, InterruptedException {
			specificReader = new SpecificDatumReader<Record>();
			wrapper = new AvroWrapper<Record>();
			this.conf = conf;
			this.ser = new HadoopSerialization(conf);
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
				return Math.min(1.0f, (getPos() - start) / (float) (end - start));
			}
		}

		public long getPos() throws IOException {
			return reader.tell();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) split;
			initialize(fileSplit.getPath(), arg1.getConfiguration());
			reader.sync(fileSplit.getStart()); // sync to start
			this.start = reader.tell();
			this.end = fileSplit.getStart() + split.getLength();
		}

		/*
		 * To be used when used externally
		 */
		public void initialize(Path path, Configuration conf) throws IOException {
			FsInput fSInput = new FsInput(path, conf);
			reader = DataFileReader.openReader(fSInput, specificReader);
			end = Long.MAX_VALUE;
			reader.sync(0);
			start = reader.tell();
		}
		
		/*
		 * To be used when used externally
		 */
		public boolean nextKeyValueNoSync() throws IOException {
			if(!reader.hasNext()) {
				return false;
			}
			wrapper.datum(reader.next(wrapper.datum()));
			if(tuple == null) {
					// Convert schema from FileReader to pangool Schema
					tuple = new Tuple(AvroUtils.toPangoolSchema(reader.getSchema()));
			}
			try {
	      AvroUtils.toTuple(wrapper.datum(), tuple, conf, ser);
      } catch(ClassNotFoundException e) {
      	throw new IOException(e);
      }
			return true;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(reader.pastSync(end)) {
				return false;
			}
			return nextKeyValueNoSync();
		}
	}

	@Override
	public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
	    throws IOException, InterruptedException {

		return new TupleInputReader(context.getConfiguration());
	}
}
