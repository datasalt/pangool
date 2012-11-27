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
package com.datasalt.pangool.tuplemr.serialization;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field.FieldConfigurable;
import com.datasalt.pangool.serialization.HadoopSerialization;

/**
 * Serializes a Tuple withing a Pangool field. Allows for tuples inside tuples.
 */
public class TupleFieldSerialization implements Serialization<ITuple>, FieldConfigurable, Configurable {

	private Schema schema;
	private HadoopSerialization ser;
	private Configuration conf;
	
	@Override
  public void setFieldProperties(Map<String, String> props) {
		schema = Schema.parse(props.get("schema"));
  }

	@Override
  public boolean accept(Class<?> argClazz) {
	  return true; // doesn't matter - to be used as field serialization
  }

	@Override
  public Deserializer<ITuple> getDeserializer(Class<ITuple> argClazz) {
	  return new SimpleTupleDeserializer(schema, ser, conf);
  }

	@Override
  public Serializer<ITuple> getSerializer(Class<ITuple> argClazz) {
	  return new SimpleTupleSerializer(schema, ser, conf);
  }

	@Override
  public Configuration getConf() {
	  return conf;
  }

	@Override
  public void setConf(Configuration conf) {
		this.conf = conf;
		try {
	    this.ser = new HadoopSerialization(conf);
    } catch(IOException e) {
	    throw new RuntimeException(e);
    }
  }
}
