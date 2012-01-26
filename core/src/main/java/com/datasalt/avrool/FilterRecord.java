package com.datasalt.avrool;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

public class FilterRecord implements GenericRecord,Comparable<FilterRecord>{

	private Schema schema;
	private GenericRecord contained;
	
	public FilterRecord(Schema schema){
		if (schema == null || !Type.RECORD.equals(schema.getType())){
      throw new AvroRuntimeException("Not a record schema: "+schema);
		}
    this.schema = schema;
	}
	
	public void setContained(GenericRecord contained){
		this.contained = contained;
	}
	
	
	@Override public boolean equals(Object o) {
    if (o == this) return true;                 // identical object
    if (!(o instanceof ProxyRecord)) return false;   // not a record
    Record that = (Record)o;
    if (!schema.getFullName().equals(that.getSchema().getFullName()))
      return false;                             // not the same schema
    return GenericData.get().compare(this, that, schema) == 0;
  }
  @Override public int hashCode() {
    return GenericData.get().hashCode(this, schema);
  }
  @Override public int compareTo(FilterRecord that) {
    return GenericData.get().compare(this, that, schema);
  }
  @Override public String toString() {
    return GenericData.get().toString(this);
  }
	@Override
  public void put(int i, Object v) {
	  // TODO Auto-generated method stub
	  
  }
	@Override
  public Object get(int i) {
	  // TODO Auto-generated method stub
	  return null;
  }
	@Override
  public Schema getSchema() {
	  // TODO Auto-generated method stub
	  return null;
  }
	@Override
  public void put(String key, Object v) {
	  // TODO Auto-generated method stub
	  
  }
	@Override
  public Object get(String key) {
	  // TODO Auto-generated method stub
	  return null;
  }
	
	
}
