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
package com.datasalt.pangolin.grouper.io.tuple.serialization;

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

import com.datasalt.pangolin.commons.io.ProtoStuffSerialization;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.Schema.Field;
import com.datasalt.pangolin.grouper.io.tuple.BaseTuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;

/**
 * A {@link Serialization} for types Tuples
 * <p>
 * To use this serialization, make sure that the Hadoop property
 * <code>io.serializations</code> includes the fully-qualified classname of this
 * class: <code>org.apache.hadoop.contrib.serialization.thrift.ThriftSerialization</code>.
 */
public class TupleSerialization implements Serialization,Configurable{
	
	private Configuration conf;
	private Schema schema;
	private com.datasalt.pangolin.io.Serialization ser;
	//private TupleDeserializer deser;
	//private TupleSerializer ser;
	
	
	public TupleSerialization(){
//		this.ser = new TupleSerializer();
//		this.deser = new TupleDeserializer();
	}
	
	@Override
  public boolean accept(Class c) {
		return (c == Tuple.class || c == BaseTuple.class);
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
			disableTupleSerialization(this.conf); // !!! MEGA TRICKY!!!!
			this.schema = Schema.parse(this.conf);
			this.ser= new com.datasalt.pangolin.io.Serialization(this.conf);
		}
		} catch(GrouperException e){
			throw new RuntimeException(e); 
		} catch(IOException e){
			throw new RuntimeException(e);
		}
	}

	@Override
	public Serializer<?> getSerializer(Class c) {
		return new TupleSerializer(this.ser,this.schema);
	}

	@Override
	public Deserializer<ITuple> getDeserializer(Class c) {
		return new TupleDeserializer(this.ser,this.schema);
	}
	
	
	 /**
		 * Caches the values from the enum fields. This is done just once for efficiency since it uses reflection. 
		 * 
		 */
		public static Map<String,Enum<?>[]> cacheEnums(Schema schema) {
			Map<String,Enum<?>[]> result = new HashMap<String,Enum<?>[]>();
			try {
				for(Field field : schema.getFields()) {
					Class<?> type = field.getType();
					if(type.isEnum()) {
						Method method = type.getMethod("values",null);
						Object values = method.invoke(null);
						result.put(field.getName(),(Enum[])values);
					}

				}
				
				return result;
			} catch(Exception e) {
				throw new RuntimeException(e);
			}

		}
		
		public static void enableTupleSerialization(Configuration conf) {
			String ser = conf.get("io.serializations").trim();
			if (ser.length() !=0 ) {
				ser += ",";
			}
			//Adding the Tuple serialization
			ser += TupleSerialization.class.getName();
			conf.set("io.serializations", ser);
	  }
		
		/**
		 * Mega tricky!!!!. This is to avoid recursive serialization instantiation!!
		 * @param conf
		 */
		public static void disableTupleSerialization(Configuration conf){
			String ser = conf.get("io.serializations").trim();
			String stToSearch = Pattern.quote("," + TupleSerialization.class.getName());
			ser = ser.replaceAll(stToSearch, "");
			conf.set("io.serializations", ser);
		}
	
}
