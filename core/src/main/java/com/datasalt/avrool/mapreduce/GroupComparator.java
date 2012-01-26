package com.datasalt.avrool.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;

public class GroupComparator extends AvroKeyComparator<Record> {

	
	public static final String CONF_GROUP_SCHEMA = "guachu_group_schema";

		private Schema schema;

		@Override
		public void setConf(Configuration conf) {
			super.setConf(conf);
			if(conf != null) {
				schema = Schema.parse(conf.get(CONF_GROUP_SCHEMA));
				System.out.println("My avro group comparator schema : " + schema);

				// schema = Pair.getKeySchema(AvroJob.getMapOutputSchema(conf));
			}
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return BinaryData.compare(b1, s1, l1, b2, s2, l2, schema);
		}

		public int compare(AvroWrapper<Record> x, AvroWrapper<Record> y) {
			return ReflectData.get().compare(x.datum(), y.datum(), schema);
		}
	}
