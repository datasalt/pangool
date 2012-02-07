package com.datasalt.pangool.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;

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
		for(Field field : pangoolSchema.getFields()) {
			org.apache.avro.Schema fieldsSchema = null;
			if(field.type().equals(String.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.STRING);
			} else if(field.type().equals(Integer.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.INT);
			} else if(field.type().equals(VIntWritable.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.INT);
			} else if(field.type().equals(Long.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.LONG);
			} else if(field.type().equals(VLongWritable.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.LONG);
			} else if(field.type().equals(Float.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.FLOAT);
			} else if(field.type().equals(Double.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.DOUBLE);
			} else if(field.type().equals(Boolean.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.BOOLEAN);
			}
			// TODO Complex types
			avroFields.add(new org.apache.avro.Schema.Field(field.name(), fieldsSchema, null, null));
		}

		org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("pangool", null, null, false);
		avroSchema.setFields(avroFields);
		return avroSchema;
	}

	public static void toTuple(Record record, ITuple tuple, org.apache.avro.Schema schema) {
		//TODO this method should reuse the tuple provided or create a new one and return it.
		// need to convert schema from Avro to Pangool
		
		
		int index = 0;
		Text text = new Text();
		for(org.apache.avro.Schema.Field field : schema.getFields()) {
			Object obj = record.get(field.name());
			if(obj instanceof Utf8) {
				Utf8 utf8 = (Utf8) obj;
				text.set(utf8.getBytes(),0,utf8.getByteLength());
				tuple.setString(index, text);
			} else if(obj instanceof Comparable) {
				tuple.set(index, obj);
			}
			// TODO Complex types
			index++;
		}
	}
}
