/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.serialization.protostuff;

import com.datasalt.pangool.serialization.protostuff.ProtoStuffDeserializer;
import com.datasalt.pangool.serialization.protostuff.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import com.dyuproject.protostuff.Schema;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProtoStuffSerialization implements Serialization<Schema> {

	@Override
  public boolean accept(Class<?> c) {
	  return Schema.class.isAssignableFrom(c);
  }

	@Override
  public Serializer<Schema> getSerializer(Class<Schema> c) {
	  return new ProtoStuffSerializer();
  }

	@Override
  public Deserializer<Schema> getDeserializer(Class<Schema> c) {
	  return new ProtoStuffDeserializer(c);
  }
	
  /**
   * Enables ProtoStuff Serialization support in Hadoop. 
   */
  public static void enableProtoStuffSerialization(Configuration conf) {
		String ser = conf.get("io.serializations").trim();
		if (ser.length() !=0 ) {
			ser += ",";
		}
		//Adding the ProtoStuff serialization
		ser += ProtoStuffSerialization.class.getName();
		conf.set("io.serializations", ser);
  }
}
