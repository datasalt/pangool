package com.datasalt.avrool;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

public class ProxyRecord implements GenericRecord,Comparable<ProxyRecord>{

	private Schema schema;
	private SerializationInfo serInfo;
	private GenericRecord contained;
	
	public ProxyRecord(SerializationInfo ser){
		this.serInfo = ser;
		this.schema = serInfo.getIntermediateSchema();
		if (schema == null || !Type.RECORD.equals(schema.getType())){
      throw new AvroRuntimeException("Not a record schema: "+schema);
		}
	}
	
	public void setContainedRecord(GenericRecord contained){
		this.contained = contained;
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
		return schema;
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
  @Override public int compareTo(ProxyRecord that) {
    return GenericData.get().compare(this, that, schema);
  }
  @Override public String toString() {
    return GenericData.get().toString(this);
  }

}
