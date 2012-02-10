package com.datasalt.pangool.io;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;

public class TupleInputFormat extends FileInputFormat<ITuple, NullWritable> {

	public static class TupleInputReader extends RecordReader<ITuple, NullWritable> {

		private SpecificDatumReader<Record> specificReader;
		private FileReader<Record> reader;
		private long start;
		private long end;

		Tuple tuple;
		AvroWrapper<Record> wrapper;

		protected TupleInputReader() throws IOException, InterruptedException {
			specificReader = new SpecificDatumReader<Record>();
			wrapper = new AvroWrapper<Record>();
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
	    if (end == start) {
	      return 0.0f;
	    } else {
	      return Math.min(1.0f, (getPos() - start) / (float)(end - start));
	    }
		}

	  public long getPos() throws IOException {
	    return reader.tell();
	  }
	  
		@Override
		public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)split;
			
			FsInput fSInput = new FsInput(fileSplit.getPath(), arg1.getConfiguration());
			reader = DataFileReader.openReader(fSInput, specificReader);
			
			reader.sync(fileSplit.getStart()); // sync to start
			this.start = reader.tell();
			this.end = fileSplit.getStart() + split.getLength();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(!reader.hasNext() || reader.pastSync(end))
				return false;
						
			wrapper.datum(reader.next(wrapper.datum()));
			if(tuple == null) {
				//TODO convert schema from FileReader to pangool Schema
				//tuple = new Tuple(reader.getSchema());
			}
			AvroUtils.toTuple(wrapper.datum(), tuple, reader.getSchema());

			return true;
		}
	}

	@Override
	public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
	    throws IOException, InterruptedException {

		return new TupleInputReader();
	}
}
