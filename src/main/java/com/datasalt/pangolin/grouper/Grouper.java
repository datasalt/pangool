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

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.TupleGroupComparator;
import com.datasalt.pangolin.grouper.io.tuple.TuplePartitioner;
import com.datasalt.pangolin.grouper.io.tuple.TupleSortComparator;
import com.datasalt.pangolin.grouper.mapreduce.Mapper;
import com.datasalt.pangolin.grouper.mapreduce.RollupCombiner;
import com.datasalt.pangolin.grouper.mapreduce.RollupReducer;
import com.datasalt.pangolin.grouper.mapreduce.SimpleCombiner;
import com.datasalt.pangolin.grouper.mapreduce.SimpleReducer;
import com.datasalt.pangolin.grouper.mapreduce.handler.MapperHandler;
import com.datasalt.pangolin.grouper.mapreduce.handler.ReducerHandler;



/**
 * GrouperWithRollup is the factory for creating MapReduce jobs that need to use  
 * TODO doc
 * @author eric
 *
 */
@SuppressWarnings("rawtypes")
public class Grouper {

	public final static String CONF_INPUT_HANDLER = "datasalt.grouper.input_handler";
	public final static String CONF_REDUCER_HANDLER = "datasalt.grouper.output_handler";
	public final static String CONF_COMBINER_HANDLER = "datasalt.grouper.combiner_handler";
	
	private Configuration conf;
	private FieldsDescription schema;
	
  private Class<? extends ReducerHandler> reducerHandler;
	private Class<? extends MapperHandler> mapperHandler; //TODO change this to multiinput
	private Class<? extends InputFormat> inputFormat;
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends ReducerHandler> combinerHandler;
	private Class<?> jarByClass;
	private Class<?> outputKeyClass,outputValueClass;
	private SortCriteria sortCriteria;
	private String [] customPartitionerFields;
	private String [] rollupBaseGroupFields,groupFields;
	
	
	public Grouper(@Nonnull Configuration conf) throws IOException{
		this.conf = conf;
		
	}
	
	public void setSortCriteria(SortCriteria sortCriteria){
		this.sortCriteria = sortCriteria;
	}
	
	public void setJarByClass(Class<?> clazz){
		this.jarByClass = clazz;
	}
	
	
	public void setSchema(FieldsDescription schema){
		this.schema = schema;
	}
	
	public void setRollupBaseGroupFields(String ... fields){
		this.rollupBaseGroupFields = fields;
	}
	
	public void setGroupFields(String ... fields){
		this.groupFields = fields;
	}
	
	public void setPartitionerFields(String ... partitionerFields){
		this.customPartitionerFields = partitionerFields;
	}
	
	public void setReducerHandler(Class<? extends ReducerHandler> reducerClass){
		this.reducerHandler = reducerClass;
	}
	
	public void setCombinerHandler(Class<? extends ReducerHandler> combinerClass){
		this.combinerHandler = combinerClass;
	}
	
	public void setMapperHandler(Class<? extends MapperHandler> mapperClass){
		this.mapperHandler = mapperClass;
	}
	
	public void setInputFormat(Class<? extends InputFormat> inputFormat){
		this.inputFormat = inputFormat;
	}
	
	public void setOutputFormat(Class<? extends OutputFormat> outputFormat){
		this.outputFormat = outputFormat;
	}
	
	public void setOutputKeyClass(Class<?> outputKeyClass){
		this.outputKeyClass = outputKeyClass;
	}
	
	public void setOutputValueClass(Class<?> outputValueClass){
		this.outputValueClass = outputValueClass;
	}
	
	
	private static String concat(String[] array,String separator){
		StringBuilder b = new StringBuilder();
		b.append(array[0]);
		for (int i=1 ; i < array.length ; i++){
			b.append(separator).append(array[i]);
		}
		
		return b.toString();
	}
	
	public Job createJob() throws IOException{
		Job job = new Job(conf);
		job.getConfiguration().set(FieldsDescription.CONF_SCHEMA,schema.serialize());
		job.getConfiguration().set(TupleGroupComparator.CONF_GROUP_COMPARATOR_FIELDS,concat(groupFields,","));
		job.getConfiguration().set(SortCriteria.CONF_SORT_CRITERIA,sortCriteria.toString());
		
		job.setInputFormatClass(inputFormat);
		job.setMapperClass(Mapper.class);
		job.getConfiguration().setClass(CONF_INPUT_HANDLER,mapperHandler,MapperHandler.class);
		
		if (rollupBaseGroupFields != null){
			//grouper with rollup
			String p = (customPartitionerFields != null) ? concat(customPartitionerFields,",") :  concat(rollupBaseGroupFields,",");
			job.getConfiguration().set(TuplePartitioner.CONF_PARTITIONER_FIELDS,p);
			job.setReducerClass(RollupReducer.class);
		} else {
			// simple grouper
			String p = (customPartitionerFields != null) ? concat(customPartitionerFields,",") :  concat(groupFields,",");
			job.getConfiguration().set(TuplePartitioner.CONF_PARTITIONER_FIELDS,p);
			job.setReducerClass(SimpleReducer.class);
		}
		
		if (combinerHandler != null){
			job.setCombinerClass((rollupBaseGroupFields == null) ? SimpleCombiner.class : RollupCombiner.class);
			job.getConfiguration().setClass(CONF_COMBINER_HANDLER,combinerHandler,ReducerHandler.class);
		}
		
		job.getConfiguration().setClass(CONF_REDUCER_HANDLER,reducerHandler,ReducerHandler.class);
		job.setInputFormatClass(inputFormat);
		job.setJarByClass((jarByClass!=null) ? jarByClass : reducerHandler);
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(TuplePartitioner.class);
		job.setGroupingComparatorClass(TupleGroupComparator.class);
		job.setSortComparatorClass(TupleSortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		return job;
	}
}
