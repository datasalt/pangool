package com.datasalt.avrool.io.records;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.SerializationInfo;
import com.datasalt.avrool.SerializationInfo.PositionMapping;

public class MapperProxyRecord implements GenericRecord,Comparable<MapperProxyRecord>{

	private Schema schema;
	private SerializationInfo serInfo;
	private GenericRecord contained;
	private FilterRecord unionRecord;
	private PositionMapping positionMapping;
	private int[] currentParticularTranslation;
	private int[] currentCommonTranslation;
	private int unionRecordPos;
	
	public MapperProxyRecord(SerializationInfo ser){
		this.serInfo = ser;
		this.schema = serInfo.getIntermediateSchema();
		this.positionMapping = serInfo.getMapperTranslation();
		//System.out.println(positionMapping);
		this.unionRecordPos = schema.getField(SerializationInfo.UNION_FIELD_NAME).pos();
		
		if (schema == null || !Type.RECORD.equals(schema.getType())){
      throw new AvroRuntimeException("Not a record schema: "+schema);
		}
		
		unionRecord = new FilterRecord();
	}
	
	public void setContainedRecord(GenericRecord contained) throws CoGrouperException{
		this.contained = contained;
		
		String source = contained.getSchema().getFullName();
		this.currentParticularTranslation = positionMapping.particularTranslation.get(source);
		this.currentCommonTranslation = positionMapping.commonTranslation.get(source);
		
		Schema particularSchema = serInfo.getParticularSchema(source);
		if (particularSchema == null){
			throw new CoGrouperException("Intermediate schema has no source '" + source + "' present in schema " + schema);
		}
		this.unionRecord.setSchema(particularSchema);
		this.unionRecord.setContained(contained,currentParticularTranslation);
		
	}
	
	@Override
  public void put(int i, Object v) {
	  throw new UnsupportedOperationException("Not able to put to this read-only record");
	  
  }

	@Override
  public Object get(int i) {
	  Field f = schema.getFields().get(i);
	  if (f.pos() == unionRecordPos){
	  	return unionRecord;
	  } else {
	  	int translatePos = currentCommonTranslation[i];
	  	return contained.get(translatePos);
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
		return get(f.pos());
//		if (f == null){
//			return null;
//		} else if (f.name().equals(SerializationInfo.UNION_FIELD_NAME)){
//			return unionRecord;
//		} else {
//			return contained.get(f.name());
//		}
  }
	
	
	@Override public boolean equals(Object o) {
    if (o == this) return true;                 // identical object
    if (!(o instanceof MapperProxyRecord)) return false;   // not a record
    Record that = (Record)o;
    if (!schema.getFullName().equals(that.getSchema().getFullName()))
      return false;                             // not the same schema
    return GenericData.get().compare(this, that, schema) == 0;
  }
  @Override public int hashCode() {
    return GenericData.get().hashCode(this, schema);
  }
  @Override public int compareTo(MapperProxyRecord that) {
    return GenericData.get().compare(this, that, schema);
  }
  @Override public String toString() {
    return GenericData.get().toString(this);
  }

}
