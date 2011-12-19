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

import com.datasalt.pangolin.grouper.io.DoubleBufferedTuple;
import com.datasalt.pangolin.grouper.io.TupleGroupComparator;
import com.datasalt.pangolin.grouper.io.TuplePartitioner;
import com.datasalt.pangolin.grouper.io.TupleSortComparator;
import com.datasalt.pangolin.grouper.mapred.GrouperMapper;
import com.datasalt.pangolin.grouper.mapred.GrouperMapperHandler;
import com.datasalt.pangolin.grouper.mapred.GrouperReducerHandler;
import com.datasalt.pangolin.grouper.mapred.GrouperWithRollupCombiner;
import com.datasalt.pangolin.grouper.mapred.GrouperWithRollupReducer;
import com.datasalt.pangolin.grouper.mapred.SimpleGrouperCombiner;
import com.datasalt.pangolin.grouper.mapred.SimpleGrouperReducer;



/**
 * GrouperWithRollup is the factory for creating MapReduce jobs that need to use  
 * TODO doc
 * @author eric
 *
 */
@SuppressWarnings("rawtypes")
public class Grouper {

	public final static String CONF_MAPPER_HANDLER = "datasalt.grouper.mapper_handler";
	public final static String CONF_REDUCER_HANDLER = "datasalt.grouper.reducer_handler";
	public final static String CONF_COMBINER_HANDLER = "datasalt.grouper.combiner_handler";
	
	private Configuration conf;
	private FieldsDescription schema;
	
  private Class<? extends GrouperReducerHandler> reducerHandler;
	private Class<? extends GrouperMapperHandler> mapperHandler; //TODO change this to multiinput
	private Class<? extends InputFormat> inputFormat;
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends GrouperReducerHandler> combinerHandler;
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
	
	public void setReducerHandler(Class<? extends GrouperReducerHandler> reducerClass){
		this.reducerHandler = reducerClass;
	}
	
	public void setCombinerHandler(Class<? extends GrouperReducerHandler> combinerClass){
		this.combinerHandler = combinerClass;
	}
	
	public void setMapperHandler(Class<? extends GrouperMapperHandler> mapperClass){
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
		job.setMapperClass(GrouperMapper.class);
		job.getConfiguration().setClass(CONF_MAPPER_HANDLER,mapperHandler,GrouperMapperHandler.class);
		
		if (rollupBaseGroupFields != null){
			//grouper with rollup
			String p = (customPartitionerFields != null) ? concat(customPartitionerFields,",") :  concat(rollupBaseGroupFields,",");
			job.getConfiguration().set(TuplePartitioner.CONF_PARTITIONER_FIELDS,p);
			job.setReducerClass(GrouperWithRollupReducer.class);
		} else {
			// simple grouper
			String p = (customPartitionerFields != null) ? concat(customPartitionerFields,",") :  concat(groupFields,",");
			job.getConfiguration().set(TuplePartitioner.CONF_PARTITIONER_FIELDS,p);
			job.setReducerClass(SimpleGrouperReducer.class);
		}
		
		if (combinerHandler != null){
			job.setCombinerClass((rollupBaseGroupFields == null) ? SimpleGrouperCombiner.class : GrouperWithRollupCombiner.class);
			job.getConfiguration().setClass(CONF_COMBINER_HANDLER,combinerHandler,GrouperReducerHandler.class);
		}
		
		job.getConfiguration().setClass(CONF_REDUCER_HANDLER,reducerHandler,GrouperReducerHandler.class);
		job.setInputFormatClass(inputFormat);
		job.setJarByClass((jarByClass!=null) ? jarByClass : reducerHandler);
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(DoubleBufferedTuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(TuplePartitioner.class);
		job.setGroupingComparatorClass(TupleGroupComparator.class);
		job.setSortComparatorClass(TupleSortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		return job;
	}
}
