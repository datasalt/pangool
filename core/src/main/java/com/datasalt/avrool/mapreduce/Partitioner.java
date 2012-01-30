package com.datasalt.avrool.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.SerializationInfo;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<AvroKey, AvroValue> implements Configurable {

	private Schema schema;
	private Configuration conf;
	
	@Override
	public int getPartition(AvroKey key, AvroValue value, int numPartitions) {
		GenericRecord record = (GenericRecord)key.datum();
		return (Integer.MAX_VALUE & GenericData.get().hashCode(record, schema)) % numPartitions;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if(conf != null) {
			this.conf = conf;
			CoGrouperConfig grouperConfig;
      try {
	      grouperConfig = CoGrouperConfig.get(conf);
	      SerializationInfo serInfo = SerializationInfo.get(grouperConfig);
	      this.schema = serInfo.getPartitionerSchema();
      } catch(CoGrouperException e) {
	     throw new RuntimeException(e);
      }
		}
	}
	
}