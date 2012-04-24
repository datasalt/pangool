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

package com.datasalt.pangool.tuplemr.avro;

import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;

import java.io.IOException;
import java.io.Serializable;
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
 * This is the Pangool's version of {@link org.apache.avro.mapred.AvroOutputFormat}.
 * It implements the new Hadoop's api in package {@link org.apache.hadoop.mapreduce.lib.output}
 * Any {@link AvroOutputFormat} instance is stateful and is not configured 
 * via {@link Configuration}. Instead, it uses Java-serialization to store its state in 
 * a Distributed Cache file.  
 */
@SuppressWarnings("serial")
public class AvroOutputFormat <T>
  extends FileOutputFormat<AvroWrapper<T>, NullWritable> implements Serializable{

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
      TaskAttemptContext job,String codecName,int deflateLevel) throws UnsupportedEncodingException {
  	Configuration conf = job.getConfiguration();
    if (FileOutputFormat.getCompressOutput(job)) {
      CodecFactory factory = codecName.equals(DEFLATE_CODEC)
        ? CodecFactory.deflateCodec(deflateLevel)
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

  private transient Schema schema;
  private String schemaStr;
  private int deflateLevel=DEFAULT_DEFLATE_LEVEL;
  private String codecName=DEFLATE_CODEC;
  
  public AvroOutputFormat(Schema schema){
  	this.schema = schema;
  	this.schemaStr = schema.toString();
  }
  
  public AvroOutputFormat(Schema schema,String codecName){
  	this(schema);
  	this.codecName = codecName;
  }
  public AvroOutputFormat(Schema schema,String codecName,int deflateLevel){
  	this(schema,codecName);
  	this.deflateLevel = deflateLevel;
  }
  
  public Schema getSchema(){
  	if (schema == null){
  		schema = new Schema.Parser().parse(schemaStr);
  	}
  	return schema;
  }
  

  @Override
  public RecordWriter<AvroWrapper<T>, NullWritable>
    getRecordWriter(TaskAttemptContext job)
    throws IOException,InterruptedException {
      
    final DataFileWriter<T> writer =
      new DataFileWriter<T>(new ReflectDatumWriter<T>());
    
    configureDataFileWriter(writer, job,codecName,deflateLevel);
    Path path = getDefaultWorkFile(job,EXT);
    writer.create(getSchema(), path.getFileSystem(job.getConfiguration()).create(path));

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