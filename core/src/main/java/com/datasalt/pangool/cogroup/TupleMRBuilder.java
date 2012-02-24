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
package com.datasalt.pangool.cogroup;

import static com.datasalt.pangool.cogroup.TupleMRException.failIfEmpty;
import static com.datasalt.pangool.cogroup.TupleMRException.failIfNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.cogroup.processors.TupleCombiner;
import com.datasalt.pangool.cogroup.processors.TupleReducer;
import com.datasalt.pangool.cogroup.processors.TupleMapper;
import com.datasalt.pangool.io.PangoolMultipleOutputs;
import com.datasalt.pangool.io.TupleInputFormat;
import com.datasalt.pangool.io.TupleOutputFormat;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.mapreduce.GroupComparator;
import com.datasalt.pangool.mapreduce.Partitioner;
import com.datasalt.pangool.mapreduce.RollupReducer;
import com.datasalt.pangool.mapreduce.SimpleCombiner;
import com.datasalt.pangool.mapreduce.SimpleReducer;
import com.datasalt.pangool.mapreduce.SortComparator;
import com.datasalt.pangool.mapreduce.lib.input.PangoolMultipleInputs;
import com.datasalt.pangool.serialization.tuples.PangoolSerialization;
import com.datasalt.pangool.utils.AvroUtils;
import com.datasalt.pangool.utils.DCUtils;

@SuppressWarnings("rawtypes")
public class TupleMRBuilder extends TupleMRConfigBuilder{

	private static final class Output {

		private String name;
		private Class<? extends OutputFormat> outputFormat;
		private Class keyClass;
		private Class valueClass;

		private Map<String, String> specificContext = new HashMap<String, String>();

		private Output(String name, Class<? extends OutputFormat> outputFormat, Class keyClass, Class valueClass,
		    Map<String, String> specificContext) {
			this.outputFormat = outputFormat;
			this.keyClass = keyClass;
			this.valueClass = valueClass;
			this.name = name;
			if(specificContext != null) {
				this.specificContext = specificContext;
			}
		}
	}

	private static final class Input {

		private Path path;
		private Class<? extends InputFormat> inputFormat;
		private TupleMapper inputProcessor;

		Input(Path path, Class<? extends InputFormat> inputFormat, TupleMapper inputProcessor) {
			this.path = path;
			this.inputFormat = inputFormat;
			this.inputProcessor = inputProcessor;
		}
	}

	private Configuration conf;
	

	private TupleReducer tupleReducer;
	private TupleCombiner tupleCombiner;
	private Class<? extends OutputFormat> outputFormat;
	private Class<?> jarByClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	private String jobName;

	private Path outputPath;

	private List<Input> multiInputs = new ArrayList<Input>();
	private List<Output> namedOutputs = new ArrayList<Output>();

	public TupleMRBuilder(Configuration conf) {
		this.conf = conf;
	}
	/**
	 * @param conf Configuration instance
	 * @param name Job's name as in {@link Job}
	 */
	public TupleMRBuilder(Configuration conf,String name){
		this.conf = conf;
		this.jobName = name;
	}

	/**
	 * Sets the jar by class , as in {@link Job#setJarByClass(Class)}
	 */
	public void setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
	}

	/**
	 * Defines an input as in {@link PangoolMultipleInputs} using {@link TupleInputFormat}
	 * @see {@link PangoolMultipleInputs}
	 */
	public void addTupleInput(Path path, TupleMapper<ITuple, NullWritable> tupleMapper) {
		this.multiInputs.add(new Input(path, TupleInputFormat.class, tupleMapper));
		AvroUtils.addAvroSerialization(conf);
		
	}
	
	/**
	 * Defines an input as in {@link PangoolMultipleInputs}
	 * @see {@link PangoolMultipleInputs}
	 */
	public void addInput(Path path, Class<? extends InputFormat> inputFormat, TupleMapper inputProcessor) {
		this.multiInputs.add(new Input(path, inputFormat, inputProcessor));
	}

	/**
	 * 
	 */
	public void setTupleCombiner(TupleCombiner tupleCombiner) {
		this.tupleCombiner = tupleCombiner;
	}

	public void setOutput(Path outputPath, Class<? extends OutputFormat> outputFormat, Class<?> outputKeyClass,
	    Class<?> outputValueClass) {
		this.outputFormat = outputFormat;
		this.outputKeyClass = outputKeyClass;
		this.outputValueClass = outputValueClass;
		this.outputPath = outputPath;
	}

	public void setTupleOutput(Path outputPath, Schema schema) {
		this.outputPath = outputPath;
		this.outputFormat = TupleOutputFormat.class;
		this.outputKeyClass = ITuple.class;
		this.outputValueClass = NullWritable.class;
		conf.set(TupleOutputFormat.CONF_TUPLE_OUTPUT_SCHEMA, schema.toString());
		AvroUtils.addAvroSerialization(conf);
	}

	public void setTupleReducer(TupleReducer tupleReducer) {
		this.tupleReducer = tupleReducer;
	}
	


	public void addNamedOutput(String namedOutput, Class<? extends OutputFormat> outputFormatClass, Class keyClass,
	    Class valueClass) throws TupleMRException {
		 addNamedOutput(namedOutput, outputFormatClass, keyClass, valueClass, null);
	}

	public void addNamedOutput(String namedOutput, Class<? extends OutputFormat> outputFormatClass, Class keyClass,
	    Class valueClass, Map<String, String> specificContext) throws TupleMRException {
		validateNamedOutput(namedOutput);
		namedOutputs.add(new Output(namedOutput, outputFormatClass, keyClass, valueClass, specificContext));
	}

	public void addNamedTupleOutput(String namedOutput, Schema outputSchema) throws TupleMRException {
		validateNamedOutput(namedOutput);
		Map<String, String> specificContext = new HashMap<String, String>();
		specificContext.put(TupleOutputFormat.CONF_TUPLE_OUTPUT_SCHEMA, outputSchema.toString());
		Output output = new Output(namedOutput, TupleOutputFormat.class, ITuple.class, NullWritable.class, specificContext);
		AvroUtils.addAvroSerialization(conf);
		namedOutputs.add(output);
	}

	private void validateNamedOutput(String namedOutput) throws TupleMRException {
		PangoolMultipleOutputs.validateOutputName(namedOutput);
		for(Output existentNamedOutput : namedOutputs) {
			if(existentNamedOutput.name.equals(namedOutput)) {
				throw new TupleMRException("Duplicate named output: " + namedOutput);
			}
		}
	}	
	
	public Job createJob() throws IOException, TupleMRException {

		failIfNull(tupleReducer, "Need to set a group handler");
		failIfEmpty(multiInputs, "Need to add at least one input");
		failIfNull(outputFormat, "Need to set output format");
		failIfNull(outputKeyClass, "Need to set outputKeyClass");
		failIfNull(outputValueClass, "Need to set outputValueClass");
		failIfNull(outputPath, "Need to set outputPath");

		TupleMRConfig grouperConf = buildConf();
		// Serialize PangoolConf in Hadoop Configuration
		TupleMRConfig.set(grouperConf, conf);
		Job job = (jobName == null) ? new Job(conf) : new Job(conf,jobName);
		if(grouperConf.getRollupFrom() != null) {
				job.setReducerClass(RollupReducer.class);
		} else {
			job.setReducerClass(SimpleReducer.class);
		}
		
		if(tupleCombiner != null) {
			job.setCombinerClass(SimpleCombiner.class); // not rollup by now
			// Set Combiner Handler
			String uniqueName = UUID.randomUUID().toString() + '.' + "combiner-handler.dat";
			try {
				DCUtils.serializeToDC(tupleCombiner, uniqueName, job.getConfiguration());
				job.getConfiguration().set(SimpleCombiner.CONF_COMBINER_HANDLER, uniqueName);
			} catch(URISyntaxException e1) {
				throw new TupleMRException(e1);
			}
		}

		// Set Group Handler
		try {
			String uniqueName = UUID.randomUUID().toString() + '.' + "group-handler.dat";
			DCUtils.serializeToDC(tupleReducer, uniqueName,job.getConfiguration());
			job.getConfiguration().set( SimpleReducer.CONF_REDUCER_HANDLER, uniqueName);
		} catch(URISyntaxException e1) {
			throw new TupleMRException(e1);
		}

		// Enabling serialization
		PangoolSerialization.enableSerialization(job.getConfiguration());

		job.setJarByClass((jarByClass != null) ? jarByClass : tupleReducer.getClass());
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(DatumWrapper.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(Partitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		FileOutputFormat.setOutputPath(job, outputPath);
		for(Input input : multiInputs) {
			PangoolMultipleInputs.addInputPath(job, input.path, input.inputFormat, input.inputProcessor);
		}
		for(Output output : namedOutputs) {
			PangoolMultipleOutputs.addNamedOutput(job, output.name, output.outputFormat, output.keyClass, output.valueClass);
			for(Map.Entry<String, String> contextKeyValue : output.specificContext.entrySet()) {
				PangoolMultipleOutputs.addNamedOutputContext(job, output.name, contextKeyValue.getKey(),
				    contextKeyValue.getValue());
			}
		}
		if(!namedOutputs.isEmpty()) {
			// Configure a {@link ProxyOutputFormat} for Pangool's Multiple Outputs to work: {@link PangoolMultipleOutput}
			try {
				job.getConfiguration().setClass(ProxyOutputFormat.PROXIED_OUTPUT_FORMAT_CONF, job.getOutputFormatClass(),
				    OutputFormat.class);
			} catch(ClassNotFoundException e) {
				// / will never happen
			}
			job.setOutputFormatClass(ProxyOutputFormat.class);
		}
		return job;
	}
}