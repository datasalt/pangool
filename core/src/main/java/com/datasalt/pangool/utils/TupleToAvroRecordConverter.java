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
package com.datasalt.pangool.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.serialization.TupleSerialization;

public class TupleToAvroRecordConverter {

	//serialization in "io.serializations"
	private HadoopSerialization hadoopSer;
	
	//custom serializers for OBJECT fields
	private Serializer[] customSerializers;
	
	private org.apache.avro.Schema avroSchema;
	private Schema pangoolSchema;
	private DataOutputBuffer[] buffers;
	private boolean schemaValidation;
	
	public TupleToAvroRecordConverter(Schema pangoolSchema,Configuration conf){
		this.pangoolSchema = pangoolSchema;
		this.avroSchema = AvroUtils.toAvroSchema(pangoolSchema);
		try{
			this.hadoopSer = new HadoopSerialization(conf);
		} catch(IOException e){
			throw new PangoolRuntimeException(e);
		}
		this.customSerializers = SerializationInfo.getSerializers(pangoolSchema,conf);
		initBuffers();
		schemaValidation = TupleSerialization.getSchemaValidation(conf);
	}
	
	private void initBuffers(){
		buffers = new DataOutputBuffer[pangoolSchema.getFields().size()];
		for (org.apache.avro.Schema.Field field : avroSchema.getFields()){
			if (field.schema().getType() == org.apache.avro.Schema.Type.BYTES){
				buffers[field.pos()] = new DataOutputBuffer();
			}
		}
		
	}
	
	/**
	 * Moves data between a Tuple and an Avro Record
	 */
	public Record toRecord(ITuple tuple, Record reuse) throws IOException {
		Record record = reuse;
		if (record == null){
			record = new Record(avroSchema);
		}
		if (schemaValidation && !tuple.getSchema().equals(pangoolSchema)){
			throw new IOException("Tuple '"+tuple + "' " +
					"contains schema not expected." +
					"Expected schema '"+ pangoolSchema + " and actual: " + tuple.getSchema());
		}
		for(int i = 0; i < pangoolSchema.getFields().size(); i++) {
			Object obj = tuple.get(i);
			Field field = pangoolSchema.getField(i);
			if (obj == null){
				throw new IOException("Field '" 
			+ field.getName() + "' can't be null in tuple:" + tuple);
			}
			
			switch(field.getType()){
			case INT:
			case LONG:
			case FLOAT:
			case BOOLEAN:
			case DOUBLE:
			case BYTES:
				record.put(i, obj); //optimistic
				break;
			case OBJECT:
				Serializer customSer = customSerializers[i];
				DataOutputBuffer buffer = buffers[i];
				buffer.reset();
				if (customSer != null){
					customSer.open(buffer);
					customSer.serialize(obj);
					customSer.close(); //TODO is this safe ?
				} else {
					hadoopSer.ser(obj, buffer);
				}
				//TODO this byteBuffer instances should be cached and reused
				ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.getData(), 0,buffer.getLength());
				record.put(i, byteBuffer);
				break;
			case ENUM:
				record.put(i,obj.toString());
				break;
			case STRING:
				record.put(i,new Utf8(obj.toString())); //could be directly String ?
				break;
			default:
					throw 
					new IOException("Not correspondence to Avro type from Pangool type " + field.getType());
			}
		}
		return record;
	}
	
}
