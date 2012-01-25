/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.io.tuple.ser;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.io.tuple.DoubleBufferedTuple;
import com.datasalt.pangool.io.tuple.ITupleInternal;

/**
 * A {@link Serialization} for types that implements {@link ITupleInternal}
 * <p>
 * To use this serialization with Hadoop, use the method
 * {@link #enableSerialization(Configuration)} over the
 * Hadoop configuration. 
 */
public class TupleInternalSerialization implements Serialization<ITupleInternal>,Configurable{
	
	private Configuration conf;
	private com.datasalt.pangool.io.Serialization ser;
	private CoGrouperConfig pangoolConfig;
	
	public TupleInternalSerialization(){
	}
	
	@Override
  public boolean accept(Class<?> c) {
		return (DoubleBufferedTuple.class.isAssignableFrom(c));
  }

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	@Override
	public void setConf(Configuration thatConf) {
		try{
			if (thatConf != null){
				this.conf = new Configuration(thatConf);
				
				// Mega tricky!!!!. This is to avoid recursive serialization instantiation!!
				disableSerialization(this.conf);
				
				this.pangoolConfig = CoGrouperConfigBuilder.get(conf);
				this.ser= new com.datasalt.pangool.io.Serialization(this.conf);
			}
		} catch(CoGrouperException e){
			throw new RuntimeException(e); 
		} catch(IOException e) {
			throw new RuntimeException(e);
		} 
	}

	@Override
	public Serializer<ITupleInternal> getSerializer(Class<ITupleInternal> c) {
		return new TupleInternalSerializer(this.ser,this.pangoolConfig);
	}

	@Override
	public Deserializer<ITupleInternal> getDeserializer(Class<ITupleInternal> c) {
		return new TupleInternalDeserializer(this.ser,this.pangoolConfig,c);
	}
	
	
	 /**
		 * Caches the values from the enum fields. This is done just once for efficiency since it uses reflection. 
		 * 
		 */
		public static Map<String,Enum<?>[]> getEnums(CoGrouperConfig pangoolConfig) {
			Map<String,Enum<?>[]> result = new HashMap<String,Enum<?>[]>();
			Schema schema = pangoolConfig.getCommonOrderedSchema();
			extractEnumsFromSchema(result, schema);
			Map<Integer,Schema> specificSchemas = pangoolConfig.getSpecificOrderedSchemas();
			for (Schema s : specificSchemas.values()){
				extractEnumsFromSchema(result,s);
			}
			return result;

		}
		
		public static void extractEnumsFromSchema(Map<String,Enum<?>[]> mapToFill,Schema schema){
			try {
				for(Field field : schema.getFields()) {
					Class<?> type = field.getType();
					if(type.isEnum()) {
						Method method = type.getMethod("values", (Class<?>[]) null);
						Object values = method.invoke(null);
						mapToFill.put(field.getName(),(Enum[])values);
					}
				}
				
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		/**
		 * Use this method to enable this serialization in Hadoop
		 */
		public static void enableSerialization(Configuration conf) {
			String serClass = TupleInternalSerialization.class.getName();
			Collection<String> currentSers = conf.getStringCollection("io.serializations");
			
			if (currentSers.size() == 0) {
				conf.set("io.serializations", serClass);
				return;
			}

			// Check if it is already present
			if (!currentSers.contains(serClass)) {
				currentSers.add(serClass);
				conf.setStrings("io.serializations", currentSers.toArray(new String[]{}));
			}
	  }
		
		/**
		 * Use this method to disable this serialization in Hadoop
		 */
		public static void disableSerialization(Configuration conf){
			String ser = conf.get("io.serializations").trim();
			String stToSearch = Pattern.quote("," + TupleInternalSerialization.class.getName());
			ser = ser.replaceAll(stToSearch, "");
			conf.set("io.serializations", ser);
		}
	
}
