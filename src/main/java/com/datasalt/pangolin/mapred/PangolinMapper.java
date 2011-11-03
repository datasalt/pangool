package com.datasalt.pangolin.mapred;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.datasalt.pangolin.io.Serialization;

/**
 * A mapper with methods for serialize and deserialize
 * 
 * @author ivan
 */
public class PangolinMapper<IKey, IValue, OKey, OValue> extends Mapper<IKey, IValue, OKey, OValue> {

	protected Serialization ser;
	
	/*
	 * For JSON Ser/De
	 */
	public final static TypeReference<HashMap<String, Object>> MAP = new TypeReference<HashMap<String, Object>>() {};
	protected ObjectMapper mapper = new ObjectMapper();
	
	/**
	 * Configures the serialization
	 */
	@Override
  protected void setup(Context context) throws IOException, InterruptedException {
	  super.setup(context);
	  
	  ser = new Serialization(context.getConfiguration());
  }
	
	/**
	 * Method for serialize. See {@link Serialization#ser(Object)} 
	 */
	public byte[] ser(Object datum) throws IOException {
		return ser.ser(datum);
	}
	
	/**
	 * Metod for deserialize. See {@link Serialization#deser(Object, BytesWritable)}
	 */
	@SuppressWarnings("unchecked")
  public <T> T deser(Object obj, BytesWritable writable) throws IOException {
		return (T) ser.deser(obj, writable);
	}
}
