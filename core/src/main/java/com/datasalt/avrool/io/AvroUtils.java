package com.datasalt.avrool.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

import com.datasalt.avrool.Schema;
import com.datasalt.avrool.Schema.Field;
import com.datasalt.avrool.io.tuple.ITuple;

public class AvroUtils {

	public static void addAvroSerialization(Configuration conf) {
		Collection<String> serializations = conf.getStringCollection("io.serializations");
		if(!serializations.contains(AvroSerialization.class.getName())) {
			serializations.add(AvroSerialization.class.getName());
			conf.setStrings("io.serializations", serializations.toArray(new String[0]));
		}
	}
	
	public static org.apache.avro.Schema toAvroSchema(Schema pangoolSchema) {
		List<org.apache.avro.Schema.Field> avroFields = new ArrayList<org.apache.avro.Schema.Field>();
		for(Field field: pangoolSchema.getFields()) {
			org.apache.avro.Schema fieldsSchema = null;
			if(field.getType().equals(String.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.STRING);
			} else if(field.getType().equals(Integer.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.INT);
			} else if(field.getType().equals(VIntWritable.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.INT);
			} else if(field.getType().equals(Long.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.LONG);
			} else if(field.getType().equals(VLongWritable.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.LONG);
			} else if(field.getType().equals(Float.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.FLOAT);
			} else if(field.getType().equals(Double.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.DOUBLE);
			} else if(field.getType().equals(Boolean.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.BOOLEAN);
			} 
			// TODO Complex types
			avroFields.add(new org.apache.avro.Schema.Field(field.getName(), fieldsSchema, null, null));
		}

		org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("pangool", null, null, false);
		avroSchema.setFields(avroFields);	
		return avroSchema;
	}
	
	public static void toTuple(Record record,ITuple tuple,org.apache.avro.Schema schema) {
		for(org.apache.avro.Schema.Field field: schema.getFields()) {
			Object obj = record.get(field.name());
			if(obj instanceof Utf8) {
				tuple.setString(field.name(), ((Utf8)obj).toString());
			} else if(obj instanceof Comparable) {
				tuple.setObject(field.name(), obj);
			}
			// TODO Complex types
		}
	}
}
