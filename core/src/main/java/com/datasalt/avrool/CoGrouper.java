package com.datasalt.avrool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.avrool.api.CombinerHandler;
import com.datasalt.avrool.api.GroupHandler;
import com.datasalt.avrool.api.GroupHandlerWithRollup;
import com.datasalt.avrool.api.InputProcessor;
import com.datasalt.avrool.io.AvroUtils;
import com.datasalt.avrool.io.TupleInputFormat;
import com.datasalt.avrool.io.TupleOutputFormat;
import com.datasalt.avrool.io.tuple.DoubleBufferedTuple;
import com.datasalt.avrool.io.tuple.ITuple;
import com.datasalt.avrool.io.tuple.ser.TupleInternalSerialization;
import com.datasalt.avrool.mapreduce.GroupComparator;
import com.datasalt.avrool.mapreduce.Partitioner;
import com.datasalt.avrool.mapreduce.RollupReducer;
import com.datasalt.avrool.mapreduce.SimpleCombiner;
import com.datasalt.avrool.mapreduce.SimpleReducer;
import com.datasalt.avrool.mapreduce.SortComparator;

@SuppressWarnings("rawtypes")
public class CoGrouper {

	private final static String CONF_REDUCER_HANDLER = CoGrouper.class.getName() + ".reducer.handler";
	private final static String CONF_COMBINER_HANDLER = CoGrouper.class.getName() + ".combiner.handler";

	private static final class Input {

		Path path;
		Class<? extends InputFormat> inputFormat;
		Class<? extends InputProcessor> inputProcessor;

		Input(Path path, Class<? extends InputProcessor<ITuple, NullWritable>> inputProcessor) {
			this.path = path;
			this.inputProcessor = inputProcessor;
			this.inputFormat = TupleInputFormat.class;
		}
		
		Input(Path path, Class<? extends InputFormat> inputFormat, Class<? extends InputProcessor> inputProcessor) {
			this.path = path;
			this.inputFormat = inputFormat;
			this.inputProcessor = inputProcessor;
		}
	}

	private Configuration conf;
	private CoGrouperConfig config;
	
	private Class<? extends GroupHandler> reduceHandler;
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends CombinerHandler> combinerHandler;
	private Class<?> jarByClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;

	private Path outputPath;

	private List<Input> multiInputs = new ArrayList<Input>();

	public CoGrouper(CoGrouperConfig config, Configuration conf) {
		this.conf = conf;
		this.config = config;
	}

	// ------------------------------------------------------------------------- //

	public CoGrouper setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
		return this;
	}

	public CoGrouper addTupleInput(Path path, Class<? extends InputProcessor<ITuple, NullWritable>> inputProcessor) {
		this.multiInputs.add(new Input(path, inputProcessor));
		AvroUtils.addAvroSerialization(conf);
		return this;
	}
	
	public CoGrouper addInput(Path path, Class<? extends InputFormat> inputFormat,
	    Class<? extends InputProcessor> inputProcessor) {
		this.multiInputs.add(new Input(path, inputFormat, inputProcessor));
		return this;
	}

	public CoGrouper setOutputHandler(Class<? extends GroupHandler> outputHandler) {
		this.reduceHandler = outputHandler;
		return this;
	}

	public CoGrouper setCombinerHandler(Class<? extends CombinerHandler> combinerHandler) {
		this.combinerHandler = combinerHandler;
		return this;
	}

	public CoGrouper setOutput(Path outputPath, Class<? extends OutputFormat> outputFormat, Class<?> outputKeyClass,
	    Class<?> outputValueClass) {
		this.outputFormat = outputFormat;
		this.outputKeyClass = outputKeyClass;
		this.outputValueClass = outputValueClass;
		this.outputPath = outputPath;
		return this;
	}
	
	public CoGrouper setTupleOutput(Path outputPath, Schema schema) {
		this.outputPath = outputPath;
		this.outputFormat = TupleOutputFormat.class;
		this.outputKeyClass = ITuple.class;
		this.outputValueClass = NullWritable.class;
		conf.set(TupleOutputFormat.CONF_TUPLE_OUTPUT_SCHEMA, schema.toString());
		AvroUtils.addAvroSerialization(conf);
		return this;
	}

	public CoGrouper setGroupHandler(Class<? extends GroupHandler> groupHandler) {
		this.reduceHandler = groupHandler;
		return this;
	}

	public CoGrouper addNamedOutput(String namedOutput, Class<? extends OutputFormat> outputFormatClass, Class keyClass, Class valueClass) {
		/*
		 * 
		 */
		return this;
	}
	
	public CoGrouper addTupleOutput(String namedOutput, Schema outputSchema) {
		/*
		 * 
		 */
		return this;
	}
	
	@SuppressWarnings("unchecked")
  public static Class<? extends GroupHandler> getGroupHandler(Configuration conf) {
		return (Class<? extends GroupHandler>) conf.getClass(CONF_REDUCER_HANDLER, null);
	}
	
	@SuppressWarnings("unchecked")
  public static Class<? extends CombinerHandler> getCombinerHandler(Configuration conf) {
		return (Class<? extends CombinerHandler>) conf.getClass(CONF_COMBINER_HANDLER, null);
	}

	// ------------------------------------------------------------------------- //

	private void raiseExceptionIfNull(Object ob, String message) throws CoGrouperException {
		if(ob == null) {
			throw new CoGrouperException(message);
		}
	}

	private void raiseExceptionIfEmpty(Collection ob, String message) throws CoGrouperException {
		if(ob == null || ob.isEmpty()) {
			throw new CoGrouperException(message);
		}
	}

	public Job createJob() throws IOException, CoGrouperException {

		raiseExceptionIfNull(reduceHandler, "Need to set a group handler");
		raiseExceptionIfEmpty(multiInputs, "Need to add at least one input");
		raiseExceptionIfNull(outputFormat, "Need to set output format");
		raiseExceptionIfNull(outputKeyClass, "Need to set outputKeyClass");
		raiseExceptionIfNull(outputValueClass, "Need to set outputValueClass");
		raiseExceptionIfNull(outputPath, "Need to set outputPath");

		if(config.getRollupFrom() != null) {
			
			// Check that rollupFrom is contained in groupBy
			
			if(!config.getGroupByFields().contains(config.getRollupFrom())) {
				throw new CoGrouperException("Rollup from [" + config.getRollupFrom() + "] not contained in group by fields "
				    + config.getGroupByFields());
			}

			// Check that we are using the appropriate Handler
			
			if(!GroupHandlerWithRollup.class.isAssignableFrom(reduceHandler)) {
				throw new CoGrouperException("Can't use " + reduceHandler + " with rollup. Please use "
				    + GroupHandlerWithRollup.class + " instead.");
			}
		}

		// Serialize PangoolConf in Hadoop Configuration
		CoGrouperConfig.setPangoolConfig(config, conf);
		Job job = new Job(conf);
		
		List<String> partitionerFields;

		if(config.getRollupFrom() != null) {
			// Grouper with rollup: calculate rollupBaseGroupFields from "rollupFrom"
			List<String> rollupBaseGroupFields = new ArrayList<String>();
			for(String groupByField : config.getGroupByFields()) {
				rollupBaseGroupFields.add(groupByField);
				if(groupByField.equals(config.getRollupFrom())) {
					break;
				}
			}
			partitionerFields = rollupBaseGroupFields;
			job.setReducerClass(RollupReducer.class);
		} else {
			// Simple grouper
			partitionerFields = config
			    .getGroupByFields();
			job.setReducerClass(SimpleReducer.class);
		}

		// Set fields to partition by in Hadoop Configuration
		Partitioner.setPartitionerFields(job.getConfiguration(), partitionerFields);

		if(combinerHandler != null) {
			job.setCombinerClass(SimpleCombiner.class); // not rollup by now 
			// Set Combiner Handler
			job.getConfiguration().setClass(CONF_COMBINER_HANDLER, combinerHandler, CombinerHandler.class);
		}
		// Set Reducer Handler
		job.getConfiguration().setClass(CONF_REDUCER_HANDLER, reduceHandler, GroupHandler.class);

		// Enabling serialization
		TupleInternalSerialization.enableSerialization(job.getConfiguration());
		
		job.setJarByClass((jarByClass != null) ? jarByClass : reduceHandler);		
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(DoubleBufferedTuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(Partitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		FileOutputFormat.setOutputPath(job, outputPath);
		for(Input input : multiInputs) {
			MultipleInputs.addInputPath(job, input.path, input.inputFormat, input.inputProcessor);
		}
		return job;
	}
}