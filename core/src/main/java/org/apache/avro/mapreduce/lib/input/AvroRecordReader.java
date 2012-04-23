package org.apache.avro.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.avro.file.FileReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectDatumReader;

/** An {@link RecordReader} for Avro data files. */
public class AvroRecordReader<T>
  extends RecordReader<AvroWrapper<T>, NullWritable> {

	private AvroWrapper<T> wrapper= new AvroWrapper<T>();
  private FileReader<T> reader;
  private long start;
  private long end;

  public AvroRecordReader(Configuration conf, FileSplit split)
    throws IOException, InterruptedException {
    initialize(split,conf);
  }
  
  @Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSplit split = (FileSplit)inputSplit;
		initialize(split,conf);
	}
	
	public void initialize(FileSplit split,Configuration conf)
			throws IOException, InterruptedException{
			init(DataFileReader.openReader
        (new FsInput(split.getPath(), conf),
         conf.getBoolean(AvroJob.INPUT_IS_REFLECT, false)
         ? new ReflectDatumReader<T>(AvroJob.getInputSchema(conf))
         : new SpecificDatumReader<T>(AvroJob.getInputSchema(conf))),
        split);
	}
	
	protected void init(FileReader<T> reader, FileSplit split)
	    throws IOException {
	    this.reader = reader;
	    reader.sync(split.getStart());                    // sync to start
	    this.start = reader.tell();
	    this.end = split.getStart() + split.getLength();
	  }

  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getPos() - start) / (float)(end - start));
    }
  }
  
  public long getPos() throws IOException {
    return reader.tell();
  }

  public void close() throws IOException { reader.close(); }

	@Override
	public AvroWrapper<T> getCurrentKey() throws IOException,
			InterruptedException {
		return wrapper;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!reader.hasNext() || reader.pastSync(end))
      return false;
    wrapper.datum(reader.next(wrapper.datum()));
    return true;
	}
  
}