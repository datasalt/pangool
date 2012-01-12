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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.Schema.Field;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;

/**
 * A {@link Serialization} for types generated by
 * <a href="http://incubator.apache.org/thrift/">Apache Thrift</a>.
 * Thrift types all descend from <code>com.facebook.thrift.TBase</code>.
 * <p>
 * To use this serialization, make sure that the Hadoop property
 * <code>io.serializations</code> includes the fully-qualified classname of this
 * class: <code>org.apache.hadoop.contrib.serialization.thrift.ThriftSerialization</code>.
 */
public class TupleSerialization implements Serialization<ITuple>,Configurable{
	
	private Configuration conf;
	private Schema schema;
	private TupleDeserializer deser;
	private TupleSerializer ser;
	
	
	public TupleSerialization(){
		this.ser = new TupleSerializer();
		this.deser = new TupleDeserializer();
	}
	
	@Override
  public boolean accept(Class<?> c) {
    return ITuple.class.isAssignableFrom(c);
  }

  

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	
	public void setConf(Configuration conf) {
		try{
		if (conf != null){
			this.conf = conf;
			this.schema = Schema.parse(conf);
			this.ser.setSchema(schema);
			this.deser.setSchema(schema);
			com.datasalt.pangolin.io.Serialization ser= new com.datasalt.pangolin.io.Serialization(conf);
		}
		} catch(GrouperException e){
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public Serializer<ITuple> getSerializer(Class<ITuple> c) {
		return ser;
	}

	@Override
	public Deserializer<ITuple> getDeserializer(Class<ITuple> c) {
		return deser;
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
						Method method = type.getMethod("values",(Class<?>)null);
						Object values = method.invoke(null);
						result.put(field.getName(),(Enum[])values);
					}

				}
				
				return result;
			} catch(Exception e) {
				throw new RuntimeException(e);
			}

		}
	
}
