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

import java.io.IOException;
import java.io.Serializable;

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

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.TupleFile;

/** An {@link org.apache.hadoop.mapreduce.OutputFormat} that writes {@link com.datasalt.pangool.io.ITuple}s. */
@SuppressWarnings("serial")
public class TupleOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements Serializable {

	private Schema outputSchema = null;

	/**
	 * Empty constructor means the output Schema will be picked from the first Tuple that is emitted. 
	 */
	public TupleOutputFormat() {
	}

	/**
	 * Providing output schema enables output validation.
	 */
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

	private CompressionCodec getCodec(TaskAttemptContext context) {
		if(getCompressOutput(context)) {
			// find the right codec
			Class<?> codecClass = SequenceFileOutputFormat.getOutputCompressorClass(context,
			    DefaultCodec.class);
			return (CompressionCodec) ReflectionUtils.newInstance(codecClass, context.getConfiguration());
		}
		return null;
	}

	public RecordWriter<ITuple, NullWritable> getRecordWriter(final TaskAttemptContext context)
	    throws IOException, InterruptedException {

		final Configuration conf = context.getConfiguration();

		final CompressionCodec codec = getCodec(context);
		final SequenceFile.CompressionType compressionType = getCompressOutput(context) ? SequenceFileOutputFormat
		    .getOutputCompressionType(context) : SequenceFile.CompressionType.NONE;
		// get the path of the temporary output file
		final Path file = getDefaultWorkFile(context, "");
		final FileSystem fs = file.getFileSystem(conf);

		return new RecordWriter<ITuple, NullWritable>() {

			TupleFile.Writer out;

			public void write(ITuple key, NullWritable value) throws IOException {
				if(out == null) {
					if(outputSchema == null) {
						outputSchema = key.getSchema();
					}
					out = new TupleFile.Writer(fs, conf, file, outputSchema, compressionType, codec, context);
				}
				out.append(key);
			}

			public void close(TaskAttemptContext context) throws IOException {
				out.close();
			}
		};
	}
}
