package com.datasalt.pangool.tuplemr.mapred.lib.input;

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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;


/**
 * An {@link org.apache.hadoop.mapreduce.RecordReader} for {@link com.datasalt.pangool.io.TupleFile}s.
 */
public class TupleFileRecordReader extends RecordReader<ITuple, NullWritable> {
  private TupleFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  private ITuple tuple = null;
  private NullWritable value = NullWritable.get();
  protected Configuration conf;
  private final Schema targetSchema;

  public TupleFileRecordReader() {
  	this(null);
  }
  
  /**
   * If a schema is specified, it will be used as target schema, trying to preserve
   * backwards compatibility always when possible.
   */
	public TupleFileRecordReader(Schema targetSchema) {
		this.targetSchema = targetSchema;
  }
  
  @Override
  public void initialize(InputSplit split,
                         TaskAttemptContext context
  ) throws IOException, InterruptedException {
    org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit =
        (org.apache.hadoop.mapreduce.lib.input.FileSplit) split;
    conf = context.getConfiguration();
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    if(targetSchema == null) {
    	this.in = new TupleFile.Reader(fs, conf, path);
    } else {
    	this.in = new TupleFile.Reader(fs, targetSchema, conf, path);
    }
    this.end = fileSplit.getStart() + fileSplit.getLength();

    if (fileSplit.getStart() > in.getPosition()) {
      in.sync(fileSplit.getStart());
    }

    this.start = in.getPosition();
    more = start < end;

    if(targetSchema == null) {
    	tuple = new Tuple(in.getSchema());
    } else {
    	tuple = new Tuple(targetSchema);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!more) {
      return false;
    }
    long pos = in.getPosition();
    boolean hasNext = in.next(tuple);
    if (!hasNext || (pos >= end && in.syncSeen())) {
      more = false;
      tuple = null;
      value = null;
    }
    return more;
  }

  @Override
  public ITuple getCurrentKey() {
    return tuple;
  }

  @Override
  public NullWritable getCurrentValue() {
    return value;
  }

  /**
   * Return the progress within the input split
   *
   * @return 0.0 to 1.0 of the input byte range
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
    }
  }

  public synchronized void close() throws IOException {
    in.close();
  }
}

