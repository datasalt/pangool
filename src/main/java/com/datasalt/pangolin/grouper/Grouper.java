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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleGroupComparator;
import com.datasalt.pangolin.grouper.io.TuplePartitioner;
import com.datasalt.pangolin.grouper.io.TupleSortComparator;
import com.datasalt.pangolin.grouper.mapred.GrouperMapper;
import com.datasalt.pangolin.grouper.mapred.SimpleGrouperCombiner;
import com.datasalt.pangolin.grouper.mapred.SimpleGrouperReducer;



/**
 * TODO
 * @author epalace
 *
 */
public class Grouper {

	
	private Configuration conf;
	private Schema schema;
	private Class<? extends SimpleGrouperReducer> reducerClass;
	private Class<? extends GrouperMapper> mapperClass; //TODO change this to multiinput
	private Class<? extends InputFormat> inputFormat;
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends SimpleGrouperCombiner> combinerClass;
	private Class<?> outputKeyClass,outputValueClass;
	private String sortCriteria;
	
	private String groupFields;
	private String partitionerFields;
	
	
	public Grouper(Configuration conf) throws IOException{
		this.conf = conf;
		
	}
	
	public void setSortCriteria(String sortCriteria){
		this.sortCriteria = sortCriteria;
	}
	
	
	public void setSchema(Schema schema){
		this.schema = schema;
	}
	
	public void setGroup(String group){
		this.groupFields = group;
		
	}
	
	
	public void setPartitionFields(String fields){
		this.partitionerFields = fields;
	}
	
	
	public void setReducerClass(Class<? extends SimpleGrouperReducer> reducerClass){
		this.reducerClass = reducerClass;
	}
	
	public void setCombinerClass(Class<? extends SimpleGrouperCombiner> combinerClass){
		this.combinerClass = combinerClass;
	}
	
	public void setMapperClass(Class<? extends GrouperMapper> mapperClass){
		this.mapperClass = mapperClass;
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
	
	
	
	
	public Job getJob() throws IOException{
		this.conf.set(Schema.CONF_SCHEMA,schema.serialize());
		this.conf.set(Constants.CONF_MIN_GROUP, groupFields);
		this.conf.set(Constants.CONF_MAX_GROUP,groupFields);
		this.conf.set(SortCriteria.CONF_SORT_CRITERIA,sortCriteria);
		this.conf.set(TuplePartitioner.CONF_PARTITIONER_FIELDS,(partitionerFields != null) ? partitionerFields :  groupFields);
		
		
		new TupleSortComparator();
		Job job = new Job(conf);
		job.setInputFormatClass(inputFormat);
		job.setMapperClass(mapperClass);
		if (combinerClass != null){
			job.setCombinerClass(combinerClass);
		}
		job.setReducerClass(reducerClass);
		job.getConfiguration().set(Schema.CONF_SCHEMA,schema.serialize());
		job.getConfiguration().set(SortCriteria.CONF_SORT_CRITERIA,sortCriteria);
		job.setInputFormatClass(inputFormat);
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
