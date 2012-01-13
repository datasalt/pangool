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
package com.datasalt.pangool.io.tuple;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;

/**
 * A {@link Serialization} for types {@link SourcedTuple}
 * <p>
 * To use this serialization, make sure that the Hadoop property
 * <code>io.serializations</code> includes the fully-qualified classname of this class
 */
public class SourcedTupleSerialization implements Serialization<ISourcedTuple>,Configurable{
	
	private Configuration conf;
	private com.datasalt.pangolin.io.Serialization ser;
	private PangoolConfig pangoolConfig;
	
	public SourcedTupleSerialization(){
	}
	
	@Override
  public boolean accept(Class<?> c) {
		return (ISourcedTuple.class.isAssignableFrom(c));
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
			disableSourcedTupleSerialization(this.conf); // !!! MEGA TRICKY!!!!
			
			this.pangoolConfig = PangoolConfigBuilder.get(conf);
			this.ser= new com.datasalt.pangolin.io.Serialization(this.conf);
		}
		} catch(CoGrouperException e){
			throw new RuntimeException(e); 
		} catch(IOException e){
			throw new RuntimeException(e);
		}
	}

	@Override
	public Serializer<ISourcedTuple> getSerializer(Class<ISourcedTuple> c) {
		return new SourcedTupleSerializer(this.ser,this.pangoolConfig);
	}

	@Override
	public Deserializer<ISourcedTuple> getDeserializer(Class<ISourcedTuple> c) {
		return new SourcedTupleDeserializer(this.ser,this.pangoolConfig,c);
	}
	
	
	 /**
		 * Caches the values from the enum fields. This is done just once for efficiency since it uses reflection. 
		 * 
		 */
		public static Map<String,Enum<?>[]> getEnums(PangoolConfig pangoolConfig) {
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
						Method method = type.getMethod("values",null);
						Object values = method.invoke(null);
						mapToFill.put(field.getName(),(Enum[])values);
					}
				}
				
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		
		public static void enableSourcedTupleSerialization(Configuration conf) {
			String ser = conf.get("io.serializations").trim();
			if (ser.length() !=0 ) {
				ser += ",";
			}
			//Adding the Tuple serialization
			ser += SourcedTupleSerialization.class.getName();
			conf.set("io.serializations", ser);
	  }
		
		/**
		 * Mega tricky!!!!. This is to avoid recursive serialization instantiation!!
		 * @param conf
		 */
		public static void disableSourcedTupleSerialization(Configuration conf){
			String ser = conf.get("io.serializations").trim();
			String stToSearch = Pattern.quote("," + SourcedTupleSerialization.class.getName());
			ser = ser.replaceAll(stToSearch, "");
			conf.set("io.serializations", ser);
		}
	
}
