/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.mapreduce.lib.output;

import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * An {@link org.apache.hadoop.mapred.OutputFormat} for Avro data files.
 * <p/>
 * You can specify various options using Job Configuration properties.
 * Look at the fields in {@link AvroJob} as well as this class to get
 * an overview of the supported options.
 */
public class AvroOutputFormat <T>
  extends FileOutputFormat<AvroWrapper<T>, NullWritable> {

  /** The file name extension for avro data files. */
  public final static String EXT = ".avro";

  /** The configuration key for Avro deflate level. */
  public static final String DEFLATE_LEVEL_KEY = "avro.mapred.deflate.level";

  /** The configuration key for Avro sync interval. */
  public static final String SYNC_INTERVAL_KEY = "avro.mapred.sync.interval";

  /** The default deflate level. */
  public static final int DEFAULT_DEFLATE_LEVEL = 1;

  /** Enable output compression using the deflate codec and specify its level.*/
  public static void setDeflateLevel(Job job, int level) {
    FileOutputFormat.setCompressOutput(job, true);
    job.getConfiguration().setInt(DEFLATE_LEVEL_KEY, level);
  }

  /** Set the sync interval to be used by the underlying {@link DataFileWriter}.*/
  public static void setSyncInterval(Job job, int syncIntervalInBytes) {
    job.getConfiguration().setInt(SYNC_INTERVAL_KEY, syncIntervalInBytes);
  }
  
  static <T> void configureDataFileWriter(DataFileWriter<T> writer,
      TaskAttemptContext job) throws UnsupportedEncodingException {
  	Configuration conf = job.getConfiguration();
    if (FileOutputFormat.getCompressOutput(job)) {
      int level = conf.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
      String codecName = conf.get(AvroJob.OUTPUT_CODEC, DEFLATE_CODEC);
      CodecFactory factory = codecName.equals(DEFLATE_CODEC)
        ? CodecFactory.deflateCodec(level)
        : CodecFactory.fromString(codecName);
      writer.setCodec(factory);
    }
    
    writer.setSyncInterval(conf.getInt(SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL));

    // copy metadata from job
    for (Map.Entry<String,String> e : conf) {
      if (e.getKey().startsWith(AvroJob.TEXT_PREFIX))
        writer.setMeta(e.getKey().substring(AvroJob.TEXT_PREFIX.length()),
                       e.getValue());
      if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
        writer.setMeta(e.getKey().substring(AvroJob.BINARY_PREFIX.length()),
                       URLDecoder.decode(e.getValue(), "ISO-8859-1")
                       .getBytes("ISO-8859-1"));
    }
  }


  @Override
  public RecordWriter<AvroWrapper<T>, NullWritable>
    getRecordWriter(TaskAttemptContext job)
    throws IOException,InterruptedException {
  	Configuration conf = job.getConfiguration();
    boolean isMapOnly = job.getNumReduceTasks() == 0;
    Schema schema = isMapOnly
      ? AvroJob.getMapOutputSchema(conf)
      : AvroJob.getOutputSchema(conf);
      
    final DataFileWriter<T> writer =
      new DataFileWriter<T>(new ReflectDatumWriter<T>());
    
    configureDataFileWriter(writer, job);
    Path path = getDefaultWorkFile(job,EXT);
    writer.create(schema, path.getFileSystem(job.getConfiguration()).create(path));

    return new RecordWriter<AvroWrapper<T>, NullWritable>() {
      @Override  
    	public void write(AvroWrapper<T> wrapper, NullWritable ignore)
          throws IOException {
          writer.append(wrapper.datum());
        }
        
        @Override
        public void close(TaskAttemptContext context) throws IOException {
          writer.close();
        }
      };
  }

}