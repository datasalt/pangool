package com.datasalt.pangool.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;

public class AvroRecordToTupleConverter {

	private Configuration conf;
	
	//serialization in "io.serializations"
	private HadoopSerialization hadoopSer;
	
	//custom deserializers for OBJECT fields
	private Deserializer[] customDeserializers;
	
	private org.apache.avro.Schema avroSchema;
	private Schema pangoolSchema;
	private DataInputBuffer inputBuffer = new DataInputBuffer();
		
	public AvroRecordToTupleConverter(org.apache.avro.Schema avroSchema,Configuration conf){
		this.avroSchema = avroSchema;
		this.pangoolSchema = AvroUtils.toPangoolSchema(avroSchema);
		this.conf = conf;
		try{
			this.hadoopSer = new HadoopSerialization(conf);
		} catch(IOException e){
			throw new PangoolRuntimeException(e);
		}
		
		this.customDeserializers = SerializationInfo.getDeserializers(pangoolSchema);
	}
	
	/**
	 * Moves data between a Record and a Tuple
	 */
	public ITuple toTuple(Record record, ITuple reuse) throws IOException {
		ITuple tuple = reuse;
		if (tuple == null){
			tuple = new Tuple(pangoolSchema);
		}
		
		Schema pangoolSchema = tuple.getSchema();
		for(org.apache.avro.Schema.Field avroField : avroSchema.getFields()) {
			int pos = avroField.pos();
			Object objRecord = record.get(pos);
			Field pangoolField = pangoolSchema.getField(pos);
			switch(pangoolField.getType()){
			case INT:
			case LONG:
			case BOOLEAN:
			case FLOAT:
			case DOUBLE:
				tuple.set(pos,objRecord); //very optimistic
				break;
			case STRING:{
				if (!(tuple.get(pos) instanceof Utf8)){
					tuple.set(pos,new com.datasalt.pangool.io.Utf8());
				}
				com.datasalt.pangool.io.Utf8 utf8=(com.datasalt.pangool.io.Utf8)tuple.get(pos);
				if (objRecord instanceof String){
					utf8.set((String)objRecord);
				} else if (objRecord instanceof Utf8){
					Utf8 avroUtf8 = (Utf8)objRecord;
					utf8.set(avroUtf8.getBytes(),0,avroUtf8.getByteLength());
				} else {
					throw new IOException("Not supported avro field " + 
							org.apache.avro.Schema.Type.STRING + " with instance " + objRecord.getClass().getName());
				}
				break;}
			case ENUM:{
					Class clazz = pangoolField.getObjectClass();
					Enum e = Enum.valueOf(clazz,objRecord.toString());
					tuple.set(pos,e);
				break;
			}
			case BYTES:
				tuple.set(pos,objRecord); //TODO FIXME this should copy bytes really, not reference!
				break;
			case OBJECT:
				Deserializer customDeser = customDeserializers[pos];
				if (objRecord instanceof byte[]){
					inputBuffer.reset((byte[])objRecord,((byte[])objRecord).length);
				} else if (objRecord instanceof ByteBuffer){
					ByteBuffer buffer = (ByteBuffer)objRecord;
					int offset = buffer.arrayOffset()+buffer.position();
					int length = buffer.limit()- buffer.position();
					inputBuffer.reset(buffer.array(),offset,length);
				} else {
					throw new PangoolRuntimeException("Can't convert to OBJECT from instance " + objRecord.getClass());
				}
				if (customDeser != null){
					customDeser.open(inputBuffer);
					tuple.set(pos,customDeser.deserialize(tuple.get(pos))); //TODO FIXME avro deserializer shouldn't reuse objects sometimes (UNION ?)
					customDeser.close(); //TODO is this ok ?
					
				} else {
						//no custom deser , then use Hadoop serializers registered in "io.serializations"
					Class clazz = pangoolField.getObjectClass();
					if(tuple.get(pos) == null || tuple.get(pos).getClass() != clazz) {
						tuple.set(pos, ReflectionUtils.newInstance(clazz, conf));
					}
					hadoopSer.deser(tuple.get(pos),inputBuffer);
				}
				break;
			default:
				throw new IOException("Not supported avro type : " + avroField.schema().getType());
			}
		}
		return tuple;
	}
	
	
}
