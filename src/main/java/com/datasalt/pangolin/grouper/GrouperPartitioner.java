package com.datasalt.pangolin.grouper;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class GrouperPartitioner extends Partitioner<Tuple,NullWritable> implements Configurable{

	private static Logger log = Logger.getLogger(GrouperPartitioner.class);
	
	private Configuration conf;
	private Schema schema;
	private int[] groupFieldsIndexes;
	
	@Override
	public int getPartition(Tuple key, NullWritable value, int numPartitions) {
		return key.partialHashCode(groupFieldsIndexes) % numPartitions;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if (conf != null){
			this.conf = conf;
			String schemaString = conf.get(Grouper.CONF_SCHEMA);
			this.schema = Schema.parse(schemaString);
		}
		
		String fieldsGroupStr = conf.get(Grouper.CONF_FIELDS_GROUP);
		//TODO do check if they match schema
		String[] fieldsGroup = fieldsGroupStr.split(",");
		
	}

	

	
}
