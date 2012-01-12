/**
 * Copyright [2011] [Datasalt Systems S.L.]
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
package com.datasalt.pangolin.grouper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.serialization.thrift.ThriftSerialization;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import com.datasalt.pangolin.commons.io.ProtoStuffSerialization;
import com.datasalt.pangolin.grouper.SortCriteria.SortElement;
import com.datasalt.pangolin.grouper.io.tuple.GroupComparator;
import com.datasalt.pangolin.grouper.io.tuple.Partitioner;
import com.datasalt.pangolin.grouper.io.tuple.SortComparator;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.serialization.TupleSerialization;
import com.datasalt.pangolin.grouper.mapreduce.InputProcessor;
import com.datasalt.pangolin.grouper.mapreduce.RollupCombiner;
import com.datasalt.pangolin.grouper.mapreduce.RollupReducer;
import com.datasalt.pangolin.grouper.mapreduce.SimpleCombiner;
import com.datasalt.pangolin.grouper.mapreduce.SimpleReducer;
import com.datasalt.pangolin.grouper.mapreduce.handler.GroupHandler;

/**
 * GrouperWithRollup is the factory for creating MapReduce jobs that need to use TODO doc
 * 
 * @author eric
 * 
 */
@SuppressWarnings("rawtypes")
public class Grouper {
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
	
	
	public final static String CONF_MAPPER_HANDLER = "datasalt.grouper.mapper_handler";
	private final static String CONF_REDUCER_HANDLER = "datasalt.grouper.reducer_handler";
	private final static String CONF_COMBINER_HANDLER = "datasalt.grouper.combiner_handler";

	private Configuration conf;
	private Schema schema;

	private Class<? extends GroupHandler> outputHandler;
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends GroupHandler> combinerHandler;
	private Class<?> jarByClass;
	private Class<?> outputKeyClass, outputValueClass;
	private SortCriteria sortCriteria;
	private String[] customPartitionerFields;
	private String[] rollupBaseGroupFields;
	private String[] fieldsToGroupBy;
	private Path outputPath;
	private List<Input> multiInputs = new ArrayList<Input>();

	private static void addTupleSerialization(Configuration conf) {
		String ser = conf.get("io.serializations").trim();
		if (ser.length() !=0 ) {
			ser += ",";
		}
		ser += TupleSerialization.class.getName();
		conf.set("io.serializations", ser);
	}
	
	public Grouper(@Nonnull Configuration conf) throws IOException {
		this.conf = conf;
		addTupleSerialization(conf);
	}

	public void addInput(Path path, Class<? extends InputFormat> inputFormat, Class<? extends InputProcessor> inputProcessor) {
		this.multiInputs.add(new Input(path, inputFormat, inputProcessor));
	}

	public void setSortCriteria(SortCriteria sortCriteria) {
		this.sortCriteria = sortCriteria;

	}

	public void setJarByClass(Class<?> clazz) {
		this.jarByClass = clazz;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public void setRollupBaseFieldsToGroupBy(String... fields) {
		this.rollupBaseGroupFields = fields;
	}

	public void setFieldsToGroupBy(String... fields) {
		this.fieldsToGroupBy = fields;
	}

	/**
	 * Optional
	 * 
	 * @param partitionerFields
	 */
	public void setPartitionerFields(String... partitionerFields) {
		this.customPartitionerFields = partitionerFields;
	}

	public void setOutputHandler(Class<? extends GroupHandler> reducerClass) {
		this.outputHandler = reducerClass;
	}

	public void setCombinerHandler(Class<? extends GroupHandler> combinerClass) {
		this.combinerHandler = combinerClass;
	}

	public void setOutputFormat(Class<? extends OutputFormat> outputFormat) {
		this.outputFormat = outputFormat;
	}

	public void setOutputKeyClass(Class<?> outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}

	public void setOutputValueClass(Class<?> outputValueClass) {
		this.outputValueClass = outputValueClass;
	}
	
	public void setOutputPath(Path outputPath){
		this.outputPath = outputPath;
	}

	private void raiseExceptionIfNull(Object ob, String message) throws GrouperException {
		if(ob == null) {
			throw new GrouperException(message);
		}
	}

	private void raiseExceptionIfEmpty(Collection ob, String message) throws GrouperException {
		if(ob == null || ob.isEmpty()) {
			throw new GrouperException(message);
		}
	}

	private void doAllChecks() throws GrouperException {

		raiseExceptionIfNull(schema, "Need to set schema");
		raiseExceptionIfNull(sortCriteria, "Need to set sort criteria");
		raiseExceptionIfNull(outputHandler, "Need to set a group handler");
		raiseExceptionIfEmpty(multiInputs, "Need to add an input");
		raiseExceptionIfNull(fieldsToGroupBy, "Need to set fields to group by");
		raiseExceptionIfNull(outputFormat, "Need to set output format");
		raiseExceptionIfNull(outputKeyClass, "Need to set outputKeyClass");
		raiseExceptionIfNull(outputValueClass, "Need to set outputValueClass");
		raiseExceptionIfNull(outputPath,"Need to set outputPath");

		// check that sortCriteria is a prefix from schema

		if(sortCriteria.getSortElements().length > schema.getFields().length) {
			throw new GrouperException("Fields in sort criteria can't exceed fields defined in schema");
		}

		// sortCriteria needs to be a prefix from schema
		for(int i = 0; i < sortCriteria.getSortElements().length; i++) {
			SortElement sortElement = sortCriteria.getSortElements()[i];
			String actualFieldName = sortElement.getFieldName();
			String expectedFieldName = schema.getField(i).getName();
			if(!actualFieldName.equals(expectedFieldName)) {
				throw new GrouperException("Sort criteria doesn't match schema (expected '" + expectedFieldName + "'  actual:'"
				    + actualFieldName + "'. " + " Sort criteria needs to be a prefix of schema");
			}
		}

//		Set<String> uniqueFields = new HashSet<String>();
//		uniqueFields.addAll(Arrays.asList(fieldsToGroupBy));
//		if(uniqueFields.size() != fieldsToGroupBy.length) {
//			throw new GrouperException("There are repeated fields in groupBy");
//		}

		if(fieldsToGroupBy.length > sortCriteria.getSortElements().length) {
			throw new GrouperException("Number of fields defined in groupBy can't exceed fields defined in sort criteria");
		}

		// check that fields defined in groupBy are defined in schema and are not repeated
		for(int i = 0; i < fieldsToGroupBy.length; i++) {
			String actualField = fieldsToGroupBy[i];
			String expectedField = schema.getField(i).getName();
			if(!actualField.equals(expectedField)) {
				throw new GrouperException("Fields defined for groupBy need to match schema. Expected:" + expectedField + ", actual:" + actualField);
			}
		}
		
		//check that fields base to rollup (if defined) are a prefix of fields in groupBy
		if (rollupBaseGroupFields != null){
			if (rollupBaseGroupFields.length > fieldsToGroupBy.length){
				throw new GrouperException("Number of fields defined in baseRollup can't exceed fields defined for groupBy");
			}
			for(int i=0 ; i < rollupBaseGroupFields.length; i++){
				String actualField = rollupBaseGroupFields[i];
				String expectedField = fieldsToGroupBy[i];
				if (!actualField.equals(expectedField)){
					throw new GrouperException("Fields defined as baseRollup need to be a prefix from groupBy . Expected field : " + expectedField + ",actual:" + actualField);
				}
			}
		}
		
	}

	public Job createJob() throws IOException, GrouperException {

		doAllChecks();

		Job job = new Job(conf);
		Schema.setInConfig(schema, job.getConfiguration());
		GroupComparator.setGroupComparatorFields(job.getConfiguration(), fieldsToGroupBy);
		SortCriteria.setInConfig(sortCriteria, job.getConfiguration());

		if(rollupBaseGroupFields != null) {
			// grouper with rollup
			String[] partitionerFields = (customPartitionerFields != null) ? customPartitionerFields : rollupBaseGroupFields;
			Partitioner.setPartitionerFields(job.getConfiguration(), partitionerFields);
			job.setReducerClass(RollupReducer.class);
		} else {
			// simple grouper
			String[] partitionerFields = (customPartitionerFields != null) ? customPartitionerFields : fieldsToGroupBy;
			Partitioner.setPartitionerFields(job.getConfiguration(), partitionerFields);
			job.setReducerClass(SimpleReducer.class);
		}

		if(combinerHandler != null) {
			// TODO this needs to be discussed
			job.setCombinerClass((rollupBaseGroupFields == null) ? SimpleCombiner.class : RollupCombiner.class);
			Grouper.setCombinerHandler(job.getConfiguration(), combinerHandler);
		}
		Grouper.setGroupHandler(job.getConfiguration(), outputHandler);
		job.setJarByClass((jarByClass != null) ? jarByClass : outputHandler);
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(Partitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		FileOutputFormat.setOutputPath(job,outputPath);
		for(Input input : multiInputs) {
			MultipleInputs.addInputPath(job, input.path, input.inputFormat, input.inputProcessor);
		}

		return job;
	}

	public static void setCombinerHandler(Configuration conf, Class<? extends GroupHandler> handler) {
		conf.setClass(CONF_COMBINER_HANDLER, handler, GroupHandler.class);
	}

	public static Class<? extends GroupHandler> getCombinerHandler(Configuration conf) {
		return (Class<? extends GroupHandler>) conf.getClass(CONF_COMBINER_HANDLER, null);
	}

	public static void setGroupHandler(Configuration conf, Class<? extends GroupHandler> handler) {
		conf.setClass(CONF_REDUCER_HANDLER, handler, GroupHandler.class);
	}

	public static Class<? extends GroupHandler> getGroupHandler(Configuration conf) {
		return (Class<? extends GroupHandler>) conf.getClass(CONF_REDUCER_HANDLER, null);
	}

}
