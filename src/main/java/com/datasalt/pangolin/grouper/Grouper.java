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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import com.datasalt.pangolin.grouper.io.TupleImpl;
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
	private Class<? extends SimpleGrouperReducer> reducerClass;
	private Map<Integer,Class<? extends GrouperMapper>> mapperClassBySource=new HashMap<Integer,Class<? extends GrouperMapper>>();
	private Map<Integer,Class<? extends InputFormat>> inputFormatBySource=new HashMap<Integer,Class<? extends InputFormat>>();
	private Map<Integer,FieldsDescription> fieldsDescriptionBySource=new HashMap<Integer,FieldsDescription>();
	private Map<Integer,Path> pathBySource = new HashMap<Integer,Path>();
	
	
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends SimpleGrouperCombiner> combinerClass;
	
	private Class<?> outputKeyClass,outputValueClass;
	private SortCriteria sortCriteria;
	
	private String groupFields;
	private String partitionerFields;
	
	
	public Grouper(Configuration conf) throws IOException{
		this.conf = conf;
		
	}
	
	public void setInput(int id,Path path,Class<? extends InputFormat> inputFormatClass,Class<? extends GrouperMapper> mapperClass,String fieldDescription) throws GrouperException{
		FieldsDescription fieldsDescription = FieldsDescription.parse(fieldDescription);
		mapperClassBySource.put(id,mapperClass);
		inputFormatBySource.put(id,inputFormatClass);
		fieldsDescriptionBySource.put(id,fieldsDescription);
		pathBySource.put(id,path);
	}
	
	
	public void setSortCriteria(String sortCriteria) throws GrouperException{
		this.sortCriteria = SortCriteria.parse(sortCriteria);
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
	
	public void setOutputFormat(Class<? extends OutputFormat> outputFormat){
		this.outputFormat = outputFormat;
	}
	
	public void setOutputKeyClass(Class<?> outputKeyClass){
		this.outputKeyClass = outputKeyClass;
	}
	
	public void setOutputValueClass(Class<?> outputValueClass){
		this.outputValueClass = outputValueClass;
	}
	
	public Job getJob() throws IOException,GrouperException {
		
		new TupleSortComparator();
		Schema schema = new Schema(fieldsDescriptionBySource,sortCriteria);
		Job job = new Job(conf);
		for (Integer sourceId : inputFormatBySource.keySet()){
			Class<? extends InputFormat> inputFormat = inputFormatBySource.get(sourceId);
			Class<? extends GrouperMapper> mapper = mapperClassBySource.get(sourceId);
			Path path = pathBySource.get(sourceId);
			MultipleInputs.addInputPath(job, path, inputFormat,mapper);
		}
		
		if (combinerClass != null){
			job.setCombinerClass(combinerClass);
		}
		job.setReducerClass(reducerClass);
		job.getConfiguration().set(FieldsDescription.CONF_SCHEMA,schema.toJson());
		job.getConfiguration().set(SortCriteria.CONF_SORT_CRITERIA,sortCriteria.toString());
		job.getConfiguration().set(Constants.CONF_MAX_GROUP,groupFields);
		job.getConfiguration().set(Constants.CONF_MIN_GROUP,groupFields);
		job.getConfiguration().set(TuplePartitioner.CONF_PARTITIONER_FIELDS,(partitionerFields != null) ? partitionerFields :  groupFields);
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(TupleImpl.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(TuplePartitioner.class);
		job.setGroupingComparatorClass(TupleGroupComparator.class);
		job.setSortComparatorClass(TupleSortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		return job;
	}

}
