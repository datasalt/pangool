package com.datasalt.avrool;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

/**
 * Filter record schema needs to be a subset of the contained record.
 * 
 * 
 */
public class FilterRecord implements GenericRecord, Comparable<FilterRecord> {

	private Schema schema; //needs to be a subset of contained schema
	private GenericRecord contained;

	public FilterRecord(){
		
	}
	
	public FilterRecord(Schema schema) {
		if(schema == null || !Type.RECORD.equals(schema.getType())) {
			throw new AvroRuntimeException("Not a record schema: " + schema);
		}
		this.schema = schema;
	}

	public void setContained(GenericRecord contained) {
		this.contained = contained;
	}
	
	public void setSchema(Schema schema){
		if(schema == null || !Type.RECORD.equals(schema.getType())) {
			throw new AvroRuntimeException("Not a record schema: " + schema);
		}
		
		this.schema = schema;
	}

	@Override
	public boolean equals(Object o) {
		if(o == this)
			return true; // identical object
		if(!(o instanceof MapOutputProxyRecord))
			return false; // not a record
		Record that = (Record) o;
		if(!schema.getFullName().equals(that.getSchema().getFullName()))
			return false; // not the same schema
		return GenericData.get().compare(this, that, schema) == 0;
	}

	@Override
	public int hashCode() {
		return GenericData.get().hashCode(this, schema);
	}

	@Override
	public int compareTo(FilterRecord that) {
		return GenericData.get().compare(this, that, schema);
	}

	@Override
	public String toString() {
		return GenericData.get().toString(this);
	}

	@Override
	public void put(int i, Object v) {
		throw new UnsupportedOperationException("Not able to put to this read-only record");
//		Field f = schema.getFields().get(i);
//		if (f == null){
//			throw new RuntimeException("Field with pos '" + i +"' not present in schema " + schema);
//		}
//		contained.put(f.name(), v);
	}

	@Override
	public Object get(int i) {
		String fieldName = schema.getFields().get(i).name();
		return contained.get(fieldName);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void put(String key, Object v) {
		throw new UnsupportedOperationException("Not able to put to this read-only record");
//		Field f = schema.getField(key);
//		if (f == null){
//			throw new RuntimeException("Field '" + key +"' not present in schema " + schema);
//		} else {
//			contained.put(key,v);
//		}
	}

	@Override
	public Object get(String key) {
		Field f = schema.getField(key);
		return (f == null) ? null : contained.get(f.name());
	}

}
