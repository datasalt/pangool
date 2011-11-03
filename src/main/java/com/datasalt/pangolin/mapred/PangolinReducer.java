package com.datasalt.pangolin.mapred;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.datasalt.pangolin.io.Serialization;

/**
 * 
 * A reducer that includes methods for serialize/deserialize
 * 
 * @author ivan
 */
public class PangolinReducer<IKey, IValue, OKey, OValue> extends Reducer<IKey, IValue, OKey, OValue> {

	/*
	 * For JSON Ser/De
	 */
	public final static TypeReference<HashMap<String, Object>> MAP = new TypeReference<HashMap<String, Object>>() {
	};
	protected ObjectMapper mapper = new ObjectMapper();

	protected Serialization ser;
	
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
