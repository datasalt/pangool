package com.datasalt.pangool.mapreduce;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.io.tuple.ITuple;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<ITuple, NullWritable> implements Configurable {

	private static final String CONF_PARTITIONER_FIELDS = Partitioner.class.getName() + ".partitioner.fields";

	private Configuration conf;
	private String[] groupFields;

	@Override
	public int getPartition(ITuple key, NullWritable value, int numPartitions) {
		return key.partialHashCode(groupFields.length) % numPartitions;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if(conf != null) {
			this.conf = conf;
			String fieldsGroupStr = conf.get(CONF_PARTITIONER_FIELDS);
			groupFields = fieldsGroupStr.split(",");
		}
	}

	public static void setPartitionerFields(Configuration conf, List<String> fields) {
		conf.setStrings(CONF_PARTITIONER_FIELDS, fields.toArray(new String[0]));
	}

	public static String[] getPartitionerFields(Configuration conf) {
		return conf.getStrings(CONF_PARTITIONER_FIELDS);
	}
}