package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangolin.grouper.io.tuple.SortComparator;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.mapreduce.InputProcessor;
import com.datasalt.pangolin.grouper.mapreduce.RollupCombiner;
import com.datasalt.pangolin.grouper.mapreduce.RollupReducer;
import com.datasalt.pangolin.grouper.mapreduce.SimpleCombiner;
import com.datasalt.pangolin.grouper.mapreduce.SimpleReducer;
import com.datasalt.pangolin.pangool.Schema.Field;
import com.datasalt.pangolin.pangool.SortCriteria.SortElement;
import com.datasalt.pangolin.pangool.io.tuple.GroupComparator;
import com.datasalt.pangolin.pangool.io.tuple.Partitioner;
import com.datasalt.pangolin.pangool.mapreduce.GroupHandler;
import com.datasalt.pangolin.pangool.mapreduce.GroupHandlerWithRollup;

/**
 * 
 * @author pere
 * 
 */
@SuppressWarnings("rawtypes")
public class CoGrouper {

	private final static String CONF_PANGOOL_CONF = CoGrouper.class.getName() + ".pangool.conf";
	private final static String CONF_REDUCER_HANDLER = CoGrouper.class.getName() + ".reducer.handler";
	private final static String CONF_COMBINER_HANDLER = CoGrouper.class.getName() + ".combiner.handler";

	/**
	 * 
	 * @author pere
	 * 
	 */
	private static final class Input {

		Path path;
		Class<? extends InputFormat> inputFormat;
		Class<? extends InputProcessor> inputProcessor;

		Input(Path path, Class<? extends InputFormat> inputFormat, Class<? extends InputProcessor> inputProcessor) {
			this.path = path;
			this.inputFormat = inputFormat;
			this.inputProcessor = inputProcessor;
		}
	}

	private PangoolConfig config;

	private Configuration conf;

	private Class<? extends GroupHandler> reduceHandler;
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends GroupHandler> combinerHandler;
	private Class<?> jarByClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;

	private Path outputPath;

	private List<Input> multiInputs = new ArrayList<Input>();

	private ObjectMapper jsonSerDe = new ObjectMapper();

	public CoGrouper(Configuration conf) {
		config = new PangoolConfig();
		this.conf = conf;
	}

	// ------------------------------------------------------------------------- //

	public CoGrouper setSorting(Sorting sorting) {
		config.setSorting(sorting);
		return this;
	}

	public CoGrouper setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
		return this;
	}

	public CoGrouper addInput(Path path, Class<? extends InputFormat> inputFormat,
	    Class<? extends InputProcessor> inputProcessor) {
		this.multiInputs.add(new Input(path, inputFormat, inputProcessor));
		return this;
	}

	public CoGrouper addSchema(Integer schemaId, String schema) throws CoGrouperException {
		return addSchema(schemaId, Schema.parse(schema));
	}

	public CoGrouper addSchema(Integer schemaId, Schema schema) throws CoGrouperException {
		if(config.getSchemes().containsKey(schemaId)) {
			throw new CoGrouperException("Schema already present: " + schemaId);
		}

		if(schema == null) {
			throw new CoGrouperException("Schema may not be null");
		}

		config.addSchema(schemaId, schema);
		return this;
	}

	public CoGrouper groupBy(String... fields) {
		config.setGroupByFields(fields);
		return this;
	}

	public CoGrouper setRollupFrom(String rollupFrom) {
		config.setRollupFrom(rollupFrom);
		return this;
	}

	public CoGrouper setPartitionerFields(String... partitionerFields) {
		config.setCustomPartitionerFields(partitionerFields);
		return this;
	}

	public CoGrouper setOutputHandler(Class<? extends GroupHandler> outputHandler) {
		this.reduceHandler = outputHandler;
		return this;
	}

	public CoGrouper setCombinerHandler(Class<? extends GroupHandler> combinerHandler) {
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

	public CoGrouper setGroupHandler(Class<? extends GroupHandler> groupHandler) {
		this.reduceHandler = groupHandler;
		return this;
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

	void doAllChecks() throws CoGrouperException {

		raiseExceptionIfEmpty(config.getSchemes().values(), "Need to set at least one schema");
		raiseExceptionIfNull(config.getSorting(), "Need to set sorting");
		raiseExceptionIfNull(config.getSorting().getSortCriteria(), "Need to set sorting criteria");
		raiseExceptionIfNull(config.getGroupByFields(), "Need to set fields to group by");
		raiseExceptionIfNull(reduceHandler, "Need to set a group handler");
		raiseExceptionIfEmpty(multiInputs, "Need to add at least one input");
		raiseExceptionIfNull(outputFormat, "Need to set output format");
		raiseExceptionIfNull(outputKeyClass, "Need to set outputKeyClass");
		raiseExceptionIfNull(outputValueClass, "Need to set outputValueClass");
		raiseExceptionIfNull(outputPath, "Need to set outputPath");

		// Check that sortCriteria is a combination of fields from schema

		List<String> commonFields = new ArrayList<String>();
		Collection<Schema> schemas = config.getSchemes().values();
		int count = 0;
		// Calculate the common fields between all schemas
		for(Schema schema : schemas) {
			if(count == 0) {
				// First schema has all common fields
				for(Field field : schema.getFields()) {
					commonFields.add(field.getName());
				}
			} else {
				// The rest of schemas are tested against the (so far) common fields
				Iterator<String> iterator = commonFields.iterator();
				while(iterator.hasNext()) {
					String field = iterator.next();
					if(!schema.containsFieldName(field)) {
						iterator.remove();
					}
				}
			}
			count++;
		}

		for(SortElement sortElement : config.getSorting().getSortCriteria().getSortElements()) {
			if(!commonFields.contains(sortElement.getFieldName())) {
				String extraReason = schemas.size() > 1 ? " common " : "";
				throw new CoGrouperException("Sort element [" + sortElement.getFieldName() + "] not contained in "
				    + extraReason + " Schema fields: " + commonFields);
			}
		}

		for(Map.Entry<Integer, SortCriteria> secondarySortCriteria : config.getSorting().getSecondarySortCriterias()
		    .entrySet()) {
			// Check that each particular sort criteria (not common fields) matches existent fields for the schema
			int schemaId = secondarySortCriteria.getKey();
			Schema schema = config.getSchemes().get(schemaId);
			if(schema == null) {
				throw new CoGrouperException("Sort criteria for unexisting schema [" + schemaId + "]");
			}
			for(SortElement sortElement : secondarySortCriteria.getValue().getSortElements()) {
				if(!schema.containsFieldName(sortElement.getFieldName())) {
					throw new CoGrouperException("Particular secondary sort for schema [" + schemaId
					    + "] has non-existent field [" + sortElement.getFieldName() + "]");
				}
			}
		}

		SortCriteria sortCriteria = config.getSorting().getSortCriteria();
		for(int i = 0; i < config.getGroupByFields().size(); i++) {
			if(!sortCriteria.getSortElements()[i].getFieldName().equals(config.getGroupByFields().get(i))) {
				throw new CoGrouperException("Group by fields " + config.getGroupByFields()
				    + " is not a prefix of sort criteria: " + sortCriteria);
			}
		}

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
	}

	public Job createJob() throws IOException, CoGrouperException {

		doAllChecks();

		Job job = new Job(conf);
		// Serialize PangoolConf in Hadoop Configuration
		conf.set(CONF_PANGOOL_CONF, config.toStringAsJSON(jsonSerDe));

		// Set fields to group by in Hadoop Configuration
		GroupComparator.setGroupComparatorFields(job.getConfiguration(), config.getGroupByFields());

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
			partitionerFields = (config.getCustomPartitionerFields() != null) ? config.getCustomPartitionerFields()
			    : rollupBaseGroupFields;
			job.setReducerClass(RollupReducer.class);
		} else {
			// Simple grouper
			partitionerFields = (config.getCustomPartitionerFields() != null) ? config.getCustomPartitionerFields() : config
			    .getGroupByFields();
			job.setReducerClass(SimpleReducer.class);
		}

		// Set fields to partition by in Hadoop Configuration
		Partitioner.setPartitionerFields(job.getConfiguration(), partitionerFields);

		if(combinerHandler != null) {
			job.setCombinerClass((config.getRollupFrom() == null) ? SimpleCombiner.class : RollupCombiner.class);
			// Set Combiner Handler
			conf.setClass(CONF_COMBINER_HANDLER, combinerHandler, GroupHandler.class);
		}
		// Set Reducer Handler
		conf.setClass(CONF_REDUCER_HANDLER, reduceHandler, GroupHandler.class);

		job.setJarByClass((jarByClass != null) ? jarByClass : reduceHandler);
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(Tuple.class);
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