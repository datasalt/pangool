package com.datasalt.pangool.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.util.ReflectionUtils;

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

	/**
	 * Converts from one Avro schema to one Pangool schema for de-serializing it
	 */
	public static Schema toPangoolSchema(org.apache.avro.Schema avroSchema) throws ClassNotFoundException {
		List<Field> fields = new ArrayList<Field>();
		for(org.apache.avro.Schema.Field field : avroSchema.getFields()) {
			Type type = field.schema().getType();
			if(type.equals(Type.STRING)) {
				fields.add(new Field(field.name(), String.class));
			} else if(type.equals(Type.INT)) {
				fields.add(new Field(field.name(), Integer.class));
			} else if(type.equals(Type.LONG)) {
				fields.add(new Field(field.name(), Long.class));
			} else if(type.equals(Type.FLOAT)) {
				fields.add(new Field(field.name(), Float.class));
			} else if(type.equals(Type.DOUBLE)) {
				fields.add(new Field(field.name(), Double.class));
			} else if(type.equals(Type.BOOLEAN)) {
				fields.add(new Field(field.name(), Boolean.class));
			} else if(type.equals(Type.BYTES)) {
				String clazz = avroSchema.getProp(field.name());
				fields.add(new Field(field.name(), Class.forName(clazz)));
			}
		}
		Schema schema = new Schema(avroSchema.getName(), fields);
		return schema;
	}

	/**
	 * Converts from one Pangool schema to one Avro schema for serializing it
	 */
	public static org.apache.avro.Schema toAvroSchema(Schema pangoolSchema) {
		List<org.apache.avro.Schema.Field> avroFields = new ArrayList<org.apache.avro.Schema.Field>();
		Map<String, String> complexTypesMetadata = new HashMap<String, String>();
		for(Field field : pangoolSchema.getFields()) {
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
			} else {
				// Complex types
				fieldsSchema = org.apache.avro.Schema.create(Type.BYTES);
				complexTypesMetadata.put(field.name(), field.getType().getName());
			}
			avroFields.add(new org.apache.avro.Schema.Field(field.name(), fieldsSchema, null, null));
		}

		org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(pangoolSchema.getName(), null, null, false);
		for(Map.Entry<String, String> props : complexTypesMetadata.entrySet()) {
			avroSchema.addProp(props.getKey(), props.getValue());
		}
		avroSchema.setFields(avroFields);
		return avroSchema;
	}

	/**
	 * Moves data between a Tuple and an Avro Record 
	 */
	public static void toRecord(Schema pangoolSchema, org.apache.avro.Schema avroSchema, ITuple tuple, Record record,
	    DataOutputBuffer tmpOutputBuffer, HadoopSerialization ser) throws IOException {
		// Convert Tuple to Record
		for(int i = 0; i < pangoolSchema.getFields().size(); i++) {
			Object obj = tuple.get(i);
			if(obj instanceof Text) {
				obj = new Utf8(((Text) obj).toString()).toString();
			}
			Field field = pangoolSchema.getField(i);
			String clazz = avroSchema.getProp(pangoolSchema.getField(i).name());
			if(clazz != null) {
				tmpOutputBuffer.reset();
				ser.ser(obj, tmpOutputBuffer);
				ByteBuffer buffer = ByteBuffer.wrap(tmpOutputBuffer.getData(),0,tmpOutputBuffer.getLength());
				record.put(field.name(), buffer);
			} else {
				record.put(field.name(), obj);
			}
		}
	}

	/**
	 * Moves data between a Record and a Tuple 
	 */
	public static void toTuple(Record record, ITuple tuple, org.apache.avro.Schema avroSchema, Configuration conf, HadoopSerialization ser) throws ClassNotFoundException, IOException {
		int index = 0;
		for(org.apache.avro.Schema.Field field : avroSchema.getFields()) {
			Object obj = record.get(field.name());
			if(obj instanceof Utf8) {
				Utf8 utf8 = (Utf8) obj;
				if(tuple.get(index) == null) {
					tuple.set(index, new Text());
				}
				((Text) tuple.get(index)).set(utf8.getBytes(), 0, utf8.getByteLength());
			} else if(obj instanceof ByteBuffer) {
				String clazz = avroSchema.getProp(field.name());
				if(clazz != null) {
					if(tuple.get(index) == null) {
						tuple.set(index, ReflectionUtils.newInstance(Class.forName(clazz), conf));
					}
					ByteBuffer byteBuffer = (ByteBuffer)obj;
					byte[] bytes = byteBuffer.array();
					ser.deser(tuple.get(index), bytes, 0, bytes.length);
				}
			} else if(obj instanceof Comparable) {
				tuple.set(index, obj);
			}
			index++;
		}
	}
}
