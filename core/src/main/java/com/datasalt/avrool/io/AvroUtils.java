package com.datasalt.avrool.io;

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.hadoop.conf.Configuration;

public class AvroUtils {

	public static void addAvroSerialization(Configuration conf) {
		Collection<String> serializations = conf.getStringCollection("io.serializations");
		if(!serializations.contains(AvroSerialization.class.getName())) {
			serializations.add(AvroSerialization.class.getName());
			conf.setStrings("io.serializations", serializations.toArray(new String[0]));
		}
	}
	
	public static Field cloneField(Field f){
		return new Field(f.name(),f.schema(),f.doc(),f.defaultValue(),f.order());
	}
	
	

}
