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
package com.datasalt.pangool.tuplemr;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.PangoolMultipleInputs;
import com.datasalt.pangool.tuplemr.mapred.lib.output.ProxyOutputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat;
import com.datasalt.pangool.utils.AvroUtils;
import com.datasalt.pangool.utils.DCUtils;

/**
 * The MapOnlyJobBuilder is a simple Pangool primitive that executes map-only
 * Jobs. You must implement {@link MapOnlyMapper} for using it.
 */
@SuppressWarnings("rawtypes")
public class MapOnlyJobBuilder {

	private Configuration conf;

	private Class<?> jarByClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	private OutputFormat outputFormat;
	private MapOnlyMapper mapOnlyMapper;

	private static final class Input {

		Path path;
		InputFormat inputFormat;

		Input(Path path, InputFormat inputFormat) {
			this.path = path;
			this.inputFormat = inputFormat;
		}
	}

	private Path outputPath;
	private List<Input> multiInputs = new ArrayList<Input>();

	public MapOnlyJobBuilder setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
		return this;
	}

	public MapOnlyJobBuilder addInput(Path path, InputFormat inputFormat) {
		this.multiInputs.add(new Input(path, inputFormat));
		return this;
	}

	public MapOnlyJobBuilder setTupleOutput(Path outputPath, Schema schema) {
		this.outputPath = outputPath;
		this.outputFormat = new TupleOutputFormat(schema.toString());
		this.outputKeyClass = ITuple.class;
		this.outputValueClass = NullWritable.class;
		AvroUtils.addAvroSerialization(conf);
		return this;
	}
	
	public MapOnlyJobBuilder setOutput(Path outputPath,
	    OutputFormat outputFormat, Class<?> outputKeyClass,
	    Class<?> outputValueClass) {
		this.outputFormat = outputFormat;
		this.outputKeyClass = outputKeyClass;
		this.outputValueClass = outputValueClass;
		this.outputPath = outputPath;
		return this;
	}

	public MapOnlyJobBuilder setMapper(MapOnlyMapper mapOnlyMapper) {
		this.mapOnlyMapper = mapOnlyMapper;
		return this;
	}

	public MapOnlyJobBuilder(Configuration conf) {
		this.conf = conf;
	}

	public Job createJob() throws IOException, TupleMRException, URISyntaxException {
		Job job = new Job(conf);
		job.setNumReduceTasks(0);

		job.setJarByClass((jarByClass != null) ? jarByClass : mapOnlyMapper.getClass());
		String uniqueName = UUID.randomUUID().toString() + '.' + "out-format.dat";
		try {
			DCUtils.serializeToDC(outputFormat, uniqueName, conf);
		} catch(URISyntaxException e1) {
			throw new TupleMRException(e1);
		}
		job.getConfiguration().set(ProxyOutputFormat.PROXIED_OUTPUT_FORMAT_CONF, uniqueName);
		job.setOutputFormatClass(ProxyOutputFormat.class);
		
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		FileOutputFormat.setOutputPath(job, outputPath);

		for(Input input : multiInputs) {
			PangoolMultipleInputs.addInputPath(job, input.path, input.inputFormat,
			    mapOnlyMapper);
		}
		return job;
	}
}
