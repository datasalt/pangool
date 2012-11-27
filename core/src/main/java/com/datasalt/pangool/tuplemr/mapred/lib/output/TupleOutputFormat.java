package com.datasalt.pangool.tuplemr.mapred.lib.output;
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
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.TupleFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.Serializable;

/** An {@link org.apache.hadoop.mapreduce.OutputFormat} that writes {@link com.datasalt.pangool.io.ITuple}s. */
public class TupleOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements Serializable {

  private Schema outputSchema;

  public TupleOutputFormat(Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  /**
   * Deprecated. Use {@link #TupleOutputFormat(com.datasalt.pangool.io.Schema)} instead.
   */
  @Deprecated
  public TupleOutputFormat(String outputSchema) {
    this.outputSchema = Schema.parse(outputSchema);
  }

  public RecordWriter<ITuple, NullWritable>
  getRecordWriter(TaskAttemptContext context
  ) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    CompressionCodec codec = null;
    SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
    if (getCompressOutput(context)) {
      // find the kind of compression to do
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);

      // find the right codec
      Class<?> codecClass = SequenceFileOutputFormat.getOutputCompressorClass(context,
          DefaultCodec.class);
      codec = (CompressionCodec)
          ReflectionUtils.newInstance(codecClass, conf);
    }

    // get the path of the temporary output file
    Path file = getDefaultWorkFile(context, "");
    FileSystem fs = file.getFileSystem(conf);
    final TupleFile.Writer out =
        new TupleFile.Writer(fs, conf, file, outputSchema,
            compressionType,
            codec,
            context);

    return new RecordWriter<ITuple, NullWritable>() {

      public void write(ITuple key, NullWritable value)
          throws IOException {
        out.append(key);
      }

      public void close(TaskAttemptContext context) throws IOException {
        out.close();
      }
    };
  }
}

