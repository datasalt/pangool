package com.datasalt.avrool.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.SerializationInfo;

public class PangoolSortComparator extends AvroKeyComparator<Record> {

		private Schema schema;

		@Override
		public void setConf(Configuration conf) {
			if(conf != null) {
				CoGrouperConfig grouperConfig;
        try {
	        grouperConfig = CoGrouperConfig.get(conf);
       
				SerializationInfo serInfo = SerializationInfo.get(grouperConfig);
				schema = serInfo.getIntermediateSchema();
        } catch(CoGrouperException e) {
	       throw new RuntimeException(e);
        }

			}
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return BinaryData.compare(b1, s1, l1, b2, s2, l2, schema);
		}

		public int compare(AvroWrapper<Record> x, AvroWrapper<Record> y) {
			return ReflectData.get().compare(x.datum(), y.datum(), schema);
		}
	}
