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

import com.datasalt.pangolin.grouper.io.tuple.GroupComparator;
import com.datasalt.pangolin.grouper.io.tuple.Partitioner;
import com.datasalt.pangolin.grouper.io.tuple.SortComparator;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.mapreduce.Mapper;
import com.datasalt.pangolin.grouper.mapreduce.RollupCombiner;
import com.datasalt.pangolin.grouper.mapreduce.RollupReducer;
import com.datasalt.pangolin.grouper.mapreduce.SimpleCombiner;
import com.datasalt.pangolin.grouper.mapreduce.SimpleReducer;
import com.datasalt.pangolin.grouper.mapreduce.handler.ReducerHandler;



/**
 * GrouperWithRollup is the factory for creating MapReduce jobs that need to use  
 * TODO doc
 * @author eric
 *
 */
@SuppressWarnings("rawtypes")
public class Grouper {

	public final static String CONF_MAPPER_HANDLER = "datasalt.grouper.mapper_handler";
	private final static String CONF_REDUCER_HANDLER = "datasalt.grouper.reducer_handler";
	private final static String CONF_COMBINER_HANDLER = "datasalt.grouper.combiner_handler";

	private Configuration conf;
	private FieldsDescription schema;
	
  private Class<? extends ReducerHandler> reducerHandler;
	private Class<? extends Mapper> mapper; //TODO change this to multiinput
	private Class<? extends InputFormat> inputFormat;
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends ReducerHandler> combinerHandler;
	private Class<?> jarByClass;
	private Class<?> outputKeyClass,outputValueClass;
	private SortCriteria sortCriteria;
	private String [] customPartitionerFields;
	private String [] rollupBaseGroupFields;
	private String[] groupFields;
	
	
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
	
	public void setMapper(Class<? extends Mapper> mapperClass){
		this.mapper = mapperClass;
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
	
	
//	private static String concat(String[] array,String separator){
//		StringBuilder b = new StringBuilder();
//		b.append(array[0]);
//		for (int i=1 ; i < array.length ; i++){
//			b.append(separator).append(array[i]);
//		}
//		
//		return b.toString();
//	}
	
	public Job createJob() throws IOException{
		Job job = new Job(conf);
		FieldsDescription.setInConfig(schema, job.getConfiguration());
		GroupComparator.setGroupComparatorFields(job.getConfiguration(),groupFields);
		SortCriteria.setInConfig(sortCriteria, job.getConfiguration());
		
		job.setInputFormatClass(inputFormat);
		job.setMapperClass(mapper);
		//Grouper.setMapperHandler(job.getConfiguration(), mapper);
		
		if (rollupBaseGroupFields != null){
			//grouper with rollup
			String[] partitionerFields = (customPartitionerFields != null) ? customPartitionerFields :  rollupBaseGroupFields;
			Partitioner.setPartitionerFields(job.getConfiguration(), partitionerFields);
			job.setReducerClass(RollupReducer.class);
		} else {
			// simple grouper
			String[] partitionerFields = (customPartitionerFields != null) ? customPartitionerFields :  groupFields;
			Partitioner.setPartitionerFields(job.getConfiguration(), partitionerFields);
			job.setReducerClass(SimpleReducer.class);
		}
		
		if (combinerHandler != null){
			//TODO this needs to be discussed
			job.setCombinerClass((rollupBaseGroupFields == null) ? SimpleCombiner.class : RollupCombiner.class);
			Grouper.setReducerHandler(job.getConfiguration(), combinerHandler);
		}
		Grouper.setReducerHandler(job.getConfiguration(), reducerHandler);
		job.setInputFormatClass(inputFormat);
		job.setJarByClass((jarByClass!=null) ? jarByClass : reducerHandler);
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(Partitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		return job;
	}
	
	
	public static void setCombinerHandler(Configuration conf, Class<? extends ReducerHandler> handler) {
		conf.setClass(CONF_COMBINER_HANDLER,handler,ReducerHandler.class);
	}

	public static Class<? extends ReducerHandler> getCombinerHandler(Configuration conf) {
		return (Class<? extends ReducerHandler>)conf.getClass(CONF_COMBINER_HANDLER,null);
	}
	
	
	public static void setReducerHandler(Configuration conf,Class<? extends ReducerHandler> handler){
		conf.setClass(CONF_REDUCER_HANDLER,handler, ReducerHandler.class);
	}
	
	public static Class<? extends ReducerHandler> getReducerHandler(Configuration conf){
		return (Class<? extends ReducerHandler>)conf.getClass(CONF_REDUCER_HANDLER,null);
	}
	
	
//	public static void setMapperHandler(Configuration conf,Class<? extends MapperHandler> handler){
//		conf.setClass(CONF_MAPPER_HANDLER, handler,MapperHandler.class);
//	}
//	
//	public static Class<? extends MapperHandler> getMapperHandler(Configuration conf){
////		String debug = conf.get(CONF_MAPPER_HANDLER);
////		System.out.println(debug);
//		return (Class<? extends MapperHandler>)conf.getClass(CONF_MAPPER_HANDLER,null);
//	}
}
