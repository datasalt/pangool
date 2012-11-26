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
import java.util.Map;
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
import com.datasalt.pangool.tuplemr.MultipleInputsInterface.Input;
import com.datasalt.pangool.tuplemr.NamedOutputsInterface.Output;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.output.ProxyOutputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat;
import com.datasalt.pangool.utils.AvroUtils;
import com.datasalt.pangool.utils.DCUtils;

/**
 * The MapOnlyJobBuilder is a simple Pangool primitive that executes map-only Jobs. You must implement
 * {@link MapOnlyMapper} for using it.
 */
@SuppressWarnings("rawtypes")
public class MapOnlyJobBuilder {

	private Configuration conf;

	private Class<?> jarByClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	private OutputFormat outputFormat;

	private Path outputPath;

	private MultipleInputsInterface multipleInputs;
	private NamedOutputsInterface namedOutputs;

	private MapOnlyMapper mapOnlyMapper;
  private String jobName = null;

  public MapOnlyJobBuilder setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
		return this;
	}

	@Deprecated
	public MapOnlyJobBuilder addInput(Path path, InputFormat inputFormat) {
		multipleInputs.getMultiInputs().add(new Input(path, inputFormat, null));
		return this;
	}

	public MapOnlyJobBuilder addInput(Path path, InputFormat inputFormat, MapOnlyMapper processor) {
		multipleInputs.getMultiInputs().add(new Input(path, inputFormat, processor));
		return this;
	}
	
	public void addNamedOutput(String namedOutput, OutputFormat outputFormat, Class keyClass,
	    Class valueClass) throws TupleMRException {
		addNamedOutput(namedOutput, outputFormat, keyClass, valueClass, null);
	}

	public void addNamedOutput(String namedOutput, OutputFormat outputFormat, Class keyClass,
	    Class valueClass, Map<String, String> specificContext) throws TupleMRException {
		namedOutputs.add(new Output(namedOutput, outputFormat, keyClass, valueClass, specificContext));
	}

	public void addNamedTupleOutput(String namedOutput, Schema outputSchema) throws TupleMRException {
		Output output = new Output(namedOutput, new TupleOutputFormat(outputSchema.toString()),
		    ITuple.class, NullWritable.class, null);
		namedOutputs.add(output);
	}

	public MapOnlyJobBuilder setTupleOutput(Path outputPath, Schema schema) {
		this.outputPath = outputPath;
		this.outputFormat = new TupleOutputFormat(schema.toString());
		this.outputKeyClass = ITuple.class;
		this.outputValueClass = NullWritable.class;
		AvroUtils.addAvroSerialization(conf);
		return this;
	}

	public MapOnlyJobBuilder setOutput(Path outputPath, OutputFormat outputFormat,
	    Class<?> outputKeyClass, Class<?> outputValueClass) {
		this.outputFormat = outputFormat;
		this.outputKeyClass = outputKeyClass;
		this.outputValueClass = outputValueClass;
		this.outputPath = outputPath;
		return this;
	}

	@Deprecated 
	public MapOnlyJobBuilder setMapper(MapOnlyMapper mapOnlyMapper) {
		this.mapOnlyMapper = mapOnlyMapper;
		return this;
	}

  public MapOnlyJobBuilder(Configuration conf) {
    this(conf, null);
  }

  public MapOnlyJobBuilder(Configuration conf, String jobName) {
		this.conf = conf;
    this.jobName = jobName;
		this.multipleInputs = new MultipleInputsInterface(conf);
		this.namedOutputs = new NamedOutputsInterface(conf);
	}

	public Job createJob() throws IOException, TupleMRException, URISyntaxException {
    Job job;
    if (jobName == null) {
		  job = new Job(conf);
    } else {
      job = new Job(conf, jobName);
    }
		job.setNumReduceTasks(0);

		String uniqueName = UUID.randomUUID().toString() + '.' + "out-format.dat";
		try {
			DCUtils.serializeToDC(outputFormat, uniqueName, conf);
		} catch(URISyntaxException e1) {
			throw new TupleMRException(e1);
		}
		job.getConfiguration().set(ProxyOutputFormat.PROXIED_OUTPUT_FORMAT_CONF, uniqueName);
		job.setOutputFormatClass(ProxyOutputFormat.class);

		if(outputKeyClass == null) {
			throw new TupleMRException("Output spec must be defined, use setOutput()");
		}
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		FileOutputFormat.setOutputPath(job, outputPath);

		Input lastInput = null;
		
		for(Input input : multipleInputs.getMultiInputs()) {
			if(input.inputProcessor == null) {
				input.inputProcessor = mapOnlyMapper;
				if(input.inputProcessor == null) {
					throw new TupleMRException("Either mapOnlyMapper property or full Input spec must be set.");
				}
			}
			lastInput = input;
		}
		
		if(lastInput == null) {
			throw new TupleMRException("At least one input must be specified");
		}
		job.setJarByClass((jarByClass != null) ? jarByClass : lastInput.inputProcessor.getClass());

		multipleInputs.configureJob(job);
		namedOutputs.configureJob(job);

		return job;
	}
}
