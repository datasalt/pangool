package com.datasalt.pangool;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.SortBy.SortElement;
import com.datasalt.pangool.api.CombinerHandler;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.GroupHandlerWithRollup;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.api.ProxyOutputFormat;
import com.datasalt.pangool.commons.DCUtils;
import com.datasalt.pangool.io.AvroUtils;
import com.datasalt.pangool.io.PangoolMultipleOutputs;
import com.datasalt.pangool.io.TupleInputFormat;
import com.datasalt.pangool.io.TupleOutputFormat;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ser.PangoolSerialization;
import com.datasalt.pangool.mapreduce.GroupComparator;
import com.datasalt.pangool.mapreduce.Partitioner;
import com.datasalt.pangool.mapreduce.RollupReducer;
import com.datasalt.pangool.mapreduce.SimpleCombiner;
import com.datasalt.pangool.mapreduce.SimpleReducer;
import com.datasalt.pangool.mapreduce.SortComparator;
import com.datasalt.pangool.mapreduce.lib.input.PangoolMultipleInputs;

@SuppressWarnings("rawtypes")
public class CoGrouper {

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
		private InputProcessor inputProcessor;

		Input(Path path, Class<? extends InputFormat> inputFormat, InputProcessor inputProcessor) {
			this.path = path;
			this.inputFormat = inputFormat;
			this.inputProcessor = inputProcessor;
		}
	}

	private Configuration conf;
	private CoGrouperConfig grouperConf;

	private GroupHandler grouperHandler;
	private CombinerHandler combinerHandler;
	private Class<? extends OutputFormat> outputFormat;
	private Class<?> jarByClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	
	private RichSortBy commonOrderBy;
	private Map<String,SortBy> secondarysOrderBy=new HashMap<String,SortBy>();

	private Path outputPath;

	private List<Input> multiInputs = new ArrayList<Input>();
	private List<Output> namedOutputs = new ArrayList<Output>();

	public CoGrouper(Configuration conf) {
		this.conf = conf;
		this.grouperConf = new CoGrouperConfig();
	}

	// ------------------------------------------------------------------------- //

	public void setOrderBy(RichSortBy ordering) {
		this.commonOrderBy = ordering;
	}
	
	public void setSecondaryOrderBy(String sourceName,SortBy ordering) {
		if (this.grouperConf.getNumSources() >=2){
			if (grouperConf.getSourceSchema(sourceName) != null){
				this.secondarysOrderBy.put(sourceName, ordering);
			} else {
				throw new IllegalStateException("No known source with name '" + sourceName + "'");
			}
		} else {
			throw new IllegalStateException("Not allowed to use secondary order with just one source");
		}
	}

	public void addSourceSchema(Schema schema) throws CoGrouperException {
		grouperConf.addSource(schema);
	}
	
	public void setGroupByFields(String... groupByFields) {
		grouperConf.setGroupByFields(groupByFields);
	}
	
	public void setRollupFrom(String rollupFrom) {
		grouperConf.setRollupFrom(rollupFrom);
	}

	public CoGrouper setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
		return this;
	}

	public CoGrouper addTupleInput(Path path, InputProcessor<ITuple, NullWritable> inputProcessor) {
		this.multiInputs.add(new Input(path, TupleInputFormat.class, inputProcessor));
		AvroUtils.addAvroSerialization(conf);
		return this;
	}

	public CoGrouper addInput(Path path, Class<? extends InputFormat> inputFormat, InputProcessor inputProcessor) {
		this.multiInputs.add(new Input(path, inputFormat, inputProcessor));
		return this;
	}

	public CoGrouper setCombinerHandler(CombinerHandler combinerHandler) {
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

	public CoGrouper setGroupHandler(GroupHandler groupHandler) {
		this.grouperHandler = groupHandler;
		return this;
	}

	public CoGrouper addNamedOutput(String namedOutput, Class<? extends OutputFormat> outputFormatClass, Class keyClass,
	    Class valueClass) throws CoGrouperException {

		return addNamedOutput(namedOutput, outputFormatClass, keyClass, valueClass, null);
	}

	public CoGrouper addNamedOutput(String namedOutput, Class<? extends OutputFormat> outputFormatClass, Class keyClass,
	    Class valueClass, Map<String, String> specificContext) throws CoGrouperException {
		validateNamedOutput(namedOutput);
		namedOutputs.add(new Output(namedOutput, outputFormatClass, keyClass, valueClass, specificContext));
		return this;
	}

	public CoGrouper addNamedTupleOutput(String namedOutput, Schema outputSchema) throws CoGrouperException {
		validateNamedOutput(namedOutput);
		Map<String, String> specificContext = new HashMap<String, String>();
		specificContext.put(TupleOutputFormat.CONF_TUPLE_OUTPUT_SCHEMA, outputSchema.toString());
		Output output = new Output(namedOutput, TupleOutputFormat.class, ITuple.class, NullWritable.class, specificContext);
		AvroUtils.addAvroSerialization(conf);
		namedOutputs.add(output);
		return this;
	}

	private void validateNamedOutput(String namedOutput) throws CoGrouperException {
		PangoolMultipleOutputs.validateOutputName(namedOutput);
		for(Output existentNamedOutput : namedOutputs) {
			if(existentNamedOutput.name.equals(namedOutput)) {
				throw new CoGrouperException("Duplicate named output: " + namedOutput);
			}
		}
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

	private static SortBy getCommonSortBy(RichSortBy richSortBy){
		if (richSortBy.getSourceOrderIndex() == null || richSortBy.getSourceOrderIndex() == richSortBy.getElements().size()){
			return new SortBy(richSortBy.getElements());
		} else {
			List<SortElement> sortElements = richSortBy.getElements().subList(0,richSortBy.getSourceOrderIndex());
			return new SortBy(sortElements);
		}
	}
	
	private static Map<String,SortBy> getSecondarySortBys(RichSortBy commonSortBy,Map<String,SortBy> secondarys){
		if (commonSortBy.getSourceOrderIndex() == null || commonSortBy.getSourceOrderIndex() == commonSortBy.getElements().size()){
			return secondarys;
		} else {
			List<SortElement> toPrepend = commonSortBy.getElements().subList(commonSortBy.getSourceOrderIndex(),commonSortBy.getElements().size());
			Map<String,SortBy> result = new HashMap<String,SortBy>();
			for (Map.Entry<String,SortBy> entry : secondarys.entrySet()){
				SortBy sortBy = entry.getValue();
				List<SortElement> newList = new ArrayList<SortElement>();
				newList.addAll(toPrepend);
				newList.addAll(sortBy.getElements());
				result.put(entry.getKey(),new SortBy(newList));
			}
			return result;
		}
	}
	
	
	public Job createJob() throws IOException, CoGrouperException {

		raiseExceptionIfNull(grouperHandler, "Need to set a group handler");
		raiseExceptionIfEmpty(multiInputs, "Need to add at least one input");
		raiseExceptionIfNull(outputFormat, "Need to set output format");
		raiseExceptionIfNull(outputKeyClass, "Need to set outputKeyClass");
		raiseExceptionIfNull(outputValueClass, "Need to set outputValueClass");
		raiseExceptionIfNull(outputPath, "Need to set outputPath");

		if(grouperConf.getRollupFrom() != null) {

			// Check that rollupFrom is contained in groupBy

			if(!grouperConf.getGroupByFields().contains(grouperConf.getRollupFrom())) {
				throw new CoGrouperException("Rollup from [" + grouperConf.getRollupFrom() + "] not contained in group by fields "
				    + grouperConf.getGroupByFields());
			}

			// Check that we are using the appropriate Handler

			if(!(grouperHandler instanceof GroupHandlerWithRollup)) {
				throw new CoGrouperException("Can't use " + grouperHandler + " with rollup. Please use "
				    + GroupHandlerWithRollup.class + " instead.");
			}
		}

		// Serialize PangoolConf in Hadoop Configuration
		
		SortBy convertedCommonOrder =getCommonSortBy(commonOrderBy);
		System.out.println("Converted common order " + convertedCommonOrder);
		grouperConf.setCommonSortBy(convertedCommonOrder);
		Map<String,SortBy> convertedParticularOrderings = getSecondarySortBys(commonOrderBy, secondarysOrderBy);
		for (Map.Entry<String,SortBy> entry : convertedParticularOrderings.entrySet()){
			System.out.println("Converted specific order " + entry);
			grouperConf.setParticularOrdering(entry.getKey(), entry.getValue());
		}
		
		
		CoGrouperConfig.set(grouperConf, conf);
		Job job = new Job(conf);

		List<String> partitionerFields;

		if(grouperConf.getRollupFrom() != null) {
			// Grouper with rollup: calculate rollupBaseGroupFields from "rollupFrom"
			List<String> rollupBaseGroupFields = new ArrayList<String>();
			for(String groupByField : grouperConf.getGroupByFields()) {
				rollupBaseGroupFields.add(groupByField);
				if(groupByField.equals(grouperConf.getRollupFrom())) {
					break;
				}
			}
			partitionerFields = rollupBaseGroupFields;
			job.setReducerClass(RollupReducer.class);
		} else {
			// Simple grouper
			partitionerFields = grouperConf.getGroupByFields();
			job.setReducerClass(SimpleReducer.class);
		}

		
		if(combinerHandler != null) {
			job.setCombinerClass(SimpleCombiner.class); // not rollup by now
			// Set Combiner Handler
			String uniqueName = UUID.randomUUID().toString() + '.' + "combiner-handler.dat";
			try {
				DCUtils.serializeToDC(combinerHandler, uniqueName, SimpleCombiner.CONF_COMBINER_HANDLER, job.getConfiguration());
			} catch(URISyntaxException e1) {
				throw new CoGrouperException(e1);
			}
		}

		// Set Group Handler
		try {
			String uniqueName = UUID.randomUUID().toString() + '.' + "group-handler.dat";
			DCUtils.serializeToDC(grouperHandler, uniqueName, SimpleReducer.CONF_REDUCER_HANDLER, job.getConfiguration());
		} catch(URISyntaxException e1) {
			throw new CoGrouperException(e1);
		}

		// Enabling serialization
		PangoolSerialization.enableSerialization(job.getConfiguration());

		job.setJarByClass((jarByClass != null) ? jarByClass : grouperHandler.getClass());
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
		if(namedOutputs.size() > 0) {
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