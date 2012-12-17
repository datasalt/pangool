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

import com.datasalt.pangool.io.ITuple;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.io.Serializable;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for reading
 * {@link com.datasalt.pangool.io.TupleFile}s containing {@link com.datasalt.pangool.io.ITuple}
 */
@SuppressWarnings("serial")
public class TupleInputFormat extends FileInputFormat<ITuple, NullWritable> implements Serializable {

  @Override
  public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit split,
                                                               TaskAttemptContext context
  ) throws IOException {
    return new TupleFileRecordReader();
  }
}

