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

import static org.apache.hadoop.io.WritableComparator.readDouble;
import static org.apache.hadoop.io.WritableComparator.readFloat;
import static org.apache.hadoop.io.WritableComparator.readVInt;
import static org.apache.hadoop.io.WritableComparator.readVLong;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.io.Schema.Field.FieldDeserializer;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.TupleMRConfig;

/**
 * A class for deserializing fields in Pangool format from a byte array.
 * Thead unsafe. It could cache the instance internally and reuse it in 
 * deserialize calls.
 */
public class SingleFieldDeserializer {

	private final HadoopSerialization hadoopSer;
	private FieldDeserializer fieldDeserializer;
	private final Type fieldType;
	private final Class<?> objectClazz;
	private Object instance;
	private final DataInputBuffer tmpInputBuffer = new DataInputBuffer();
	
  public SingleFieldDeserializer(Configuration conf, TupleMRConfig mrConfig,
  		Type fieldType,Class<?> objectClazz,FieldDeserializer deser) throws IOException {
		this.hadoopSer = new HadoopSerialization(conf);
		this.fieldType = fieldType;
		this.fieldDeserializer = deser;
		this.objectClazz = objectClazz;
		switch(fieldType){
			case STRING: this.instance = new Utf8(); break;
			case OBJECT:
				this.instance = ReflectionUtils.newInstance(objectClazz, conf);
				break;
				//TODO this code is commented because no array copy is performed.
//			case BYTES:
//				this.instance = ByteBuffer.allocate(512);//initial capacity
//				break;
			default:
				this.instance=null;
		}
	} 
	
	/**
	 * Deserialize an individual field from a byte array position that is encoded with the 
	 * {@link TupleSerialization}.
	 * 
	 * @param bytes The byte array.
	 * @param offset The place to start reading.
	 * @return A deserialized instance. 
	 * 				    
	 */
	public Object deserialize(byte[] bytes, int offset) throws IOException {
		//TODO repeated code from TupleDeserializer. REFACTOR!!
		switch(fieldType){
		case INT: return readVInt(bytes, offset);
		case LONG: return readVLong(bytes, offset);			
		case FLOAT:	return readFloat(bytes, offset);
		case DOUBLE: return readDouble(bytes, offset);
		case BOOLEAN: return bytes[offset] != 0;
		case ENUM: 
			int value1 = readVInt(bytes, offset);
			return objectClazz.getEnumConstants()[value1];
		case STRING:
  		int length = readVInt(bytes, offset);
  		offset += WritableUtils.decodeVIntSize(bytes[offset]);
  		((Utf8) instance).set(bytes, offset, length);
  		return instance;
		case BYTES:
			length = readVInt(bytes, offset);
			offset += WritableUtils.decodeVIntSize(bytes[offset]);
			return ByteBuffer.wrap(bytes,offset,length);
		case OBJECT: 
  		length = readVInt(bytes, offset); //read prepended length
  		offset += WritableUtils.decodeVIntSize(bytes[offset]);
  		if (fieldDeserializer == null){
  			instance = hadoopSer.deser(instance, bytes, offset, length);
  			return instance;
  		} else {
  			tmpInputBuffer.reset(bytes,offset,length);
  			fieldDeserializer.open(tmpInputBuffer);
  			instance = fieldDeserializer.deserialize(instance);
  			fieldDeserializer.close();
  			return instance;
  		}
  	 default:
  		throw new IOException("Not supported type:" + fieldType); 
		}
  }	
}
