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

public class TupleToAvroRecordConverter {

	private Configuration conf;
	
	//serialization in "io.serializations"
	private HadoopSerialization hadoopSer;
	
	//custom serializers for OBJECT fields
	private Serializer[] customSerializers;
	
	private org.apache.avro.Schema avroSchema;
	private Schema pangoolSchema;
	private DataOutputBuffer[] buffers;
	
	public TupleToAvroRecordConverter(Schema pangoolSchema,Configuration conf){
		this.pangoolSchema = pangoolSchema;
		this.avroSchema = AvroUtils.toAvroSchema(pangoolSchema);
		this.conf = conf;
		try{
			this.hadoopSer = new HadoopSerialization(conf);
		} catch(IOException e){
			throw new PangoolRuntimeException(e);
		}
		this.customSerializers = SerializationInfo.getSerializers(pangoolSchema,conf);
		initBuffers();
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
		for(int i = 0; i < pangoolSchema.getFields().size(); i++) {
			Object obj = tuple.get(i);
			Field field = pangoolSchema.getField(i);
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
