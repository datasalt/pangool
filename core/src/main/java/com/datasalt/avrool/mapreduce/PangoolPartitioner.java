package com.datasalt.avrool.mapreduce;

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.PangoolKey;
import com.datasalt.avrool.SerializationInfo;

public class PangoolPartitioner extends org.apache.hadoop.mapreduce.Partitioner<PangoolKey,NullWritable> implements Configurable {

	private Schema schema;
	private Configuration conf;
	private GenericData genericData = GenericData.get();
	
	@Override
	public int getPartition(PangoolKey key, NullWritable value, int numPartitions) {
		//GenericRecord record = (GenericRecord)key.datum();
		//return (Integer.MAX_VALUE & genericData.hashCode(record, schema)) % numPartitions;
		return (Integer.MAX_VALUE & hashCode(key.datum(), schema)) % numPartitions;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if(conf != null) {
			this.conf = conf;
			CoGrouperConfig grouperConfig;
      try {
	      grouperConfig = CoGrouperConfig.get(conf);
	      SerializationInfo serInfo = SerializationInfo.get(grouperConfig);
	      this.schema = serInfo.getPartitionerSchema();
      } catch(CoGrouperException e) {
	     throw new RuntimeException(e);
      }
		}
	}
	
	
	 /** Compute a hash code according to a schema, consistent with {@link
   * #compare(Object,Object,Schema)}. */
  public int hashCode(Object o, Schema s) {
    if (o == null) return 0;                      // incomplete datum
    int hashCode = 1;
    switch (s.getType()) {
    case RECORD:
      for (Field f : s.getFields()) {
        if (f.order() == Field.Order.IGNORE){
          continue;
        }
//        hashCode = hashCodeAdd(hashCode,
//                               getField(o, f.name(), f.pos()), f.schema());
        hashCode = hashCodeAdd(hashCode,((GenericRecord)o).get(f.pos()),f.schema());
      }
      return hashCode;
    case ARRAY:
      Collection<?> a = (Collection<?>)o;
      Schema elementType = s.getElementType();
      for (Object e : a)
        hashCode = hashCodeAdd(hashCode, e, elementType);
      return hashCode;
    case UNION:
      return hashCode(o, s.getTypes().get(genericData.resolveUnion(s, o)));
    case ENUM:
      return s.getEnumOrdinal(o.toString());
    case NULL:
      return 0;
    case STRING:
    	//TODO see if this matches Avro Utf8 hashCode
    	//intended to not instantiate Utf8 
      return (o instanceof Utf8 ? o.hashCode() : hashStringBytes((String)o));
    default:
      return o.hashCode();
    }
  }

  /** Add the hash code for an object into an accumulated hash code. */
  protected int hashCodeAdd(int hashCode, Object o, Schema s) {
    return 31*hashCode + hashCode(o, s);
  }
  
  protected int hashStringBytes(String s){
  	return hashBytes(s.getBytes());
  }
  
  protected int hashBytes(byte[] bytes) {
    int hash = 0;
    for (int i = 0; i < bytes.length; i++)
      hash = hash*31 + bytes[i];
    return hash;
  }
  
	
}