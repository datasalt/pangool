package com.datasalt.avrool;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

public class GroupHandlerProxyRecord implements GenericRecord,Comparable<GroupHandlerProxyRecord>{

	private Schema destinationSchema;
	private Schema commonSchema;
	private CoGrouperConfig config;
	private GenericRecord contained;
	private GenericRecord unionRecord;
	
	public GroupHandlerProxyRecord(CoGrouperConfig  config){
			this.config = config;
		try{
			this.commonSchema = SerializationInfo.get(config).getCommonSchema();
		} catch(CoGrouperException e){
			throw new RuntimeException(e);
		}
	}
	
	public void setContainedRecord(GenericRecord contained) throws CoGrouperException{
		this.contained = contained;
		this.unionRecord = (GenericRecord)contained.get(SerializationInfo.UNION_FIELD_NAME);
		String source = unionRecord.getSchema().getFullName();
		this.destinationSchema = config.getSchemaBySource(source);
		if (this.destinationSchema == null){
			throw new CoGrouperException("Not known source with name '" + source + "'");
		}
		

	}
	
	@Override
  public void put(int i, Object v) {
	  throw new UnsupportedOperationException("Not able to put to this read-only record");
	  
  }

	@Override
  public Object get(int i) {
	  Field f = destinationSchema.getFields().get(i);
	  return (f == null) ? null : get(f.name());
  }

	@Override
  public Schema getSchema() {
		return destinationSchema;
  }

	@Override
  public void put(String key, Object v) {
		throw new UnsupportedOperationException("Not able to put to this read-only record");
	  
  }

	@Override
  public Object get(String key) {
		if (commonSchema.getField(key) != null){
	  	return contained.get(key);
	  } else {
	  	return unionRecord.get(key);
	  }
  }
	
	
	@Override public boolean equals(Object o) {
    if (o == this) return true;                 // identical object
    if (!(o instanceof GroupHandlerProxyRecord)) return false;   // not a record
    Record that = (Record)o;
    if (!destinationSchema.getFullName().equals(that.getSchema().getFullName()))
      return false;                             // not the same intermediateSchema
    return GenericData.get().compare(this, that, destinationSchema) == 0;
  }
  @Override public int hashCode() {
    return GenericData.get().hashCode(this, destinationSchema);
  }
  @Override public int compareTo(GroupHandlerProxyRecord that) {
    return GenericData.get().compare(this, that, destinationSchema);
  }
  @Override public String toString() {
    return GenericData.get().toString(this);
  }

}
