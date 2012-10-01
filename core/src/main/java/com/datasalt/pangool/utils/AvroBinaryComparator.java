package com.datasalt.pangool.utils;

import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

@SuppressWarnings({ "serial", "rawtypes" })
public class AvroBinaryComparator implements RawComparator, Serializable,Configurable{
	
	private transient Schema schema;
	private String schemaStr;
	
	
	public AvroBinaryComparator(Schema schema){
		this.schema = schema;
		this.schemaStr = schema.toString();
	}

	@SuppressWarnings("deprecation")
  public void setConf(Configuration notUsed){
		schema = Schema.parse(schemaStr);
	}
	
	public Configuration getConf(){
		return null;
	}
	
	@Override
  public int compare(Object object1, Object object2) {
	  throw new NotImplementedException();
  }

	@Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return BinaryData.compare(b1, s1, l1, b2, s2, l2, schema);
  }

}
