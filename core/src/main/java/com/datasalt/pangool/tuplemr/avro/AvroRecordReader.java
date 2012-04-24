package com.datasalt.pangool.tuplemr.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * This is the pangool's implementation of {@link org.apache.avro.mapred.AvroRecordReader}.
 * It implements the new Hadoop's api defined in package {@link org.apache.hadoop.mapreduce.lib.output}
 * Any {@link AvroOutputFormat} instance is stateful and is not configured 
 * via {@link Configuration}. Instead, it uses Java-serialization to store its state in 
 * a Distributed Cache file. 
 * 
 */
public class AvroRecordReader<T>
  extends RecordReader<AvroWrapper<T>, NullWritable> {

	private AvroWrapper<T> wrapper= new AvroWrapper<T>();
  private FileReader<T> reader;
  private long start;
  private long end;
  
  private Schema schema;
  private boolean isReflect;

  public AvroRecordReader(Schema schema,boolean isReflect,Configuration conf, FileSplit split)
    throws IOException, InterruptedException {
  	this.schema = schema;
  	this.isReflect = isReflect;
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
         isReflect
         ? new ReflectDatumReader<T>(schema)
         : new SpecificDatumReader<T>(schema)),
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
		if (!reader.hasNext() || reader.pastSync(end)){
      return false;
		}
		
    wrapper.datum(reader.next(wrapper.datum()));
    return true;
	}
  
}