package com.datasalt.avrool.io.records;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.SerializationInfo;
import com.datasalt.avrool.SerializationInfo.PositionMapping;

/**
 * 
 * This record wraps the records recevied in the reducer-step and then projects them to the original source schema specified in {@link CoGrouperConfig} 
 *
 */
public class ReducerProxyRecord implements GenericRecord,Comparable<ReducerProxyRecord>{

	private Schema schema;
	private CoGrouperConfig config;
	private SerializationInfo serInfo;
	private GenericRecord contained;
	private GenericRecord unionRecord;
	private PositionMapping posMapping;
	private int[] currentCommonMapping;
	private int[] currentParticularMapping;
	
	public ReducerProxyRecord(CoGrouperConfig  config){
			this.config = config;
		try{
			this.serInfo = SerializationInfo.get(config);
			this.posMapping = serInfo.getReducerTranslation();
		} catch(CoGrouperException e){
			throw new RuntimeException(e);
		}
	}
	
	public void setContainedRecord(GenericRecord contained) throws CoGrouperException{
		this.contained = contained;
		this.unionRecord = (GenericRecord)contained.get(SerializationInfo.UNION_FIELD_NAME);
		String source = unionRecord.getSchema().getFullName();
		this.schema = config.getSchemaBySource(source);
		if (this.schema == null){
			throw new CoGrouperException("Not known source with name '" + source + "'");
		}
		
		this.currentCommonMapping = posMapping.commonTranslation.get(source);
		this.currentParticularMapping = posMapping.particularTranslation.get(source);
		
		
	}
	
	@Override
  public void put(int i, Object v) {
	  throw new UnsupportedOperationException("Not able to put to this read-only record");
	  
  }

	@Override
  public Object get(int i) {
		int commonPos = currentCommonMapping[i];
		if (commonPos >=0){
			return contained.get(commonPos);
		} else {
			int particularPos = currentParticularMapping[i];
			return unionRecord.get(particularPos);
		}

  }

	@Override
  public Schema getSchema() {
		return schema;
  }

	@Override
  public void put(String key, Object v) {
		throw new UnsupportedOperationException("Not able to put to this read-only record");
	  
  }

	@Override
  public Object get(String key) {
		Field f = schema.getField(key);
		return (f == null) ? null : get(f.pos());
  }
	
	
	@Override public boolean equals(Object o) {
    if (o == this) return true;                 // identical object
    if (!(o instanceof ReducerProxyRecord)) return false;   // not a record
    Record that = (Record)o;
    if (!schema.getFullName().equals(that.getSchema().getFullName()))
      return false;                             // not the same intermediateSchema
    return GenericData.get().compare(this, that, schema) == 0;
  }
  @Override public int hashCode() {
    return GenericData.get().hashCode(this, schema);
  }
  @Override public int compareTo(ReducerProxyRecord that) {
    return GenericData.get().compare(this, that, schema);
  }
  @Override public String toString() {
    return GenericData.get().toString(this);
  }

}
