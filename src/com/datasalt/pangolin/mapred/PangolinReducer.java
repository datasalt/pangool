package com.datasalt.pangolin.mapred;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;


import org.apache.hadoop.io.BytesWritable;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.datasalt.pangolin.commons.PangolinGuiceModule;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.mapred.InjectedReducer;
import com.google.inject.Module;

/**
 * A mapper with all ready to be injected with the {@link PisaeGuiceModule}.
 * It also includes methods for serialize/deserialize
 * 
 * @author ivan
 */
public class PangolinReducer<IKey, IValue, OKey, OValue> extends InjectedReducer<IKey, IValue, OKey, OValue> {

	/*
	 * For JSON Ser/De
	 */
	public final static TypeReference<HashMap<String, Object>> MAP = new TypeReference<HashMap<String, Object>>() {
	};
	protected ObjectMapper mapper = new ObjectMapper();

	@Override
  protected List<Module> getGuiceModules(Context context) {
		PangolinGuiceModule pisaeModule = new PangolinGuiceModule();
		Iterator<Entry<String, String>> it = context.getConfiguration().iterator();
		while (it.hasNext()) {
			Entry<String, String> entry = it.next();
			pisaeModule.getConfigProperties().put(entry.getKey(), entry.getValue());
		}
		return Arrays.asList(new Module[]{ pisaeModule });
  }
	
	private Serialization ser;
	
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
