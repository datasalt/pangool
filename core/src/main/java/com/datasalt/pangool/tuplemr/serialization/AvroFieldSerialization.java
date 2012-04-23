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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.io.Schema.Field.FieldSerialization;

public class AvroFieldSerialization<T> extends FieldSerialization<T>{
	
	private Schema schema;
	private boolean isReflect;
	
	public AvroFieldSerialization(){
		
	}
	
	@Override
  public Deserializer<T> getDeserializer(Class<T> clazz) {
		return new AvroFieldDeserializer<T>(schema, isReflect);
  }

	@Override
  public Serializer<T> getSerializer(Class<T> clazz) {
	  return new AvroFieldSerializer<T>(schema, isReflect);
  }

	@Override
  public void setFieldProps(Map<String,String> properties) {
		schema = Schema.parse(properties.get("avro.schema"));
		String r = properties.get("avro.reflection");
		isReflect = (r != null) && Boolean.parseBoolean(r);
  }
	
	
	public static class AvroFieldSerializer<T> implements Serializer<T>{

		private DatumWriter<T> writer;
    private OutputStream out;
    private BinaryEncoder encoder;

    public AvroFieldSerializer(Schema schema,boolean isReflect){
    writer = (isReflect) ?
    		new ReflectDatumWriter<T>(schema)
    		: new SpecificDatumWriter<T>(schema);
    }
    
		@Override
		public void open(OutputStream out) throws IOException {
			this.out = out;
      this.encoder = new EncoderFactory().configureBlockSize(512)
          .binaryEncoder(out, null);
		}

		@Override
		public void serialize(T obj) throws IOException {
			writer.write(obj, encoder);
      // would be a lot faster if the Serializer interface had a flush()
      // method and the Hadoop framework called it when needed rather
      // than for every record.
      encoder.flush();
			
		}
		
		@Override
		public void close() throws IOException {
			out.close();
		}
	}
	
	public static class AvroFieldDeserializer<T> implements Deserializer<T>{

		private static final DecoderFactory FACTORY = DecoderFactory.get();
		private DatumReader<T> reader;
    private BinaryDecoder decoder;
    
    public AvroFieldDeserializer(Schema schema,boolean isReflect){
    	reader = (isReflect) ? new ReflectDatumReader<T>(schema)
    				: new SpecificDatumReader<T>(schema);
    }
		
		@Override
		public void close() throws IOException {
			decoder.inputStream().close();
		}

		@Override
		public T deserialize(T obj) throws IOException {
			return reader.read(obj, decoder);
		}

		@Override
		public void open(InputStream in) throws IOException {
			this.decoder = FACTORY.directBinaryDecoder(in, decoder);
		}
	}
	
}
