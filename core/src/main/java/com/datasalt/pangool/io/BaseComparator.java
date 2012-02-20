package com.datasalt.pangool.io;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.io.tuple.ser.SingleFieldDeserializer;

@SuppressWarnings("serial")
public abstract class BaseComparator<T> implements RawComparator<T>, Serializable, Configurable {

	private Configuration conf;
	private SingleFieldDeserializer fieldDeser;
	private final Class<T> objectClass;
  private T object1 = null;
  private T object2 = null;
  
	public BaseComparator(Class<T> objectClass) {
		this.objectClass = objectClass;
	}
	
	@Override
	public void setConf(Configuration conf) {
		try {
	    fieldDeser = new SingleFieldDeserializer(conf, CoGrouperConfig.get(conf));
	    	    
    } catch(IOException e) {
    	throw new RuntimeException(e);
    } catch(CoGrouperException e) {
    	throw new RuntimeException(e);
    }
	}
	
	@Override
  public Configuration getConf() {
		return conf;
  }

	@Override
  public abstract int compare(T o1, T o2);
	
	@SuppressWarnings("unchecked")
  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {

	    object1 = (T) fieldDeser.deserialize(object1, b1, s1, objectClass);	    
	    object2 = (T) fieldDeser.deserialize(object2, b2, s2, objectClass);

		} catch(IOException e) {
			throw new RuntimeException(e);
    }

	  return compare(object1, object2);
  }	
}
