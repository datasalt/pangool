package com.datasalt.pangool.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;

import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.io.tuple.ITuple;

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
			}
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
			}
			// etc
		}
	}
}
