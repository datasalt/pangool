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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.utils.Buffer;


public class TupleDeserializer implements Deserializer<DatumWrapper<ITuple>> {

	private static class CachedTuples {
		private ITuple commonTuple;
		private List<ITuple> specificTuples=new ArrayList<ITuple>();
		private List<ITuple> resultTuples=new ArrayList<ITuple>();
	}
	
	
	private final TupleMRConfig tupleMRConf;
	private final Configuration conf;
	private final SerializationInfo serInfo;
	private  DataInputStream in;
	private final HadoopSerialization ser;
	private final boolean isRollup;
	private final boolean multipleSources;
	private final Map<Class<?>, Enum<?>[]> cachedEnums;

	private final Buffer tmpInputBuffer = new Buffer();
	private DatumWrapper<CachedTuples> cachedTuples = new DatumWrapper<CachedTuples>();

	public TupleDeserializer(HadoopSerialization ser, TupleMRConfig tupleMRConfig, Configuration conf) {
		this.tupleMRConf = tupleMRConfig;
		this.conf = conf;
		this.serInfo = tupleMRConf.getSerializationInfo();
		this.ser = ser;
		this.cachedEnums = TupleSerialization.getEnums(tupleMRConfig);
		this.isRollup = tupleMRConf.getRollupFrom() != null && !tupleMRConf.getRollupFrom().isEmpty();
		this.multipleSources = tupleMRConf.getNumIntermediateSchemas() >= 2;
		this.cachedTuples.datum(createCachedTuples(tupleMRConf));
		this.cachedTuples.swapInstances(); //do rollup
		this.cachedTuples.datum(createCachedTuples(tupleMRConf));
		
	}
	
	private static CachedTuples createCachedTuples(TupleMRConfig config){
		SerializationInfo serInfo = config.getSerializationInfo();
		boolean multipleSources = config.getNumIntermediateSchemas() >= 2;
		CachedTuples r = new CachedTuples();
		r.commonTuple = new Tuple(serInfo.getCommonSchema()); 
		for (Schema sourceSchema : config.getIntermediateSchemas()){
			r.resultTuples.add(new Tuple(sourceSchema));
		}
		
		if (multipleSources){
			for(Schema specificSchema : serInfo.getSpecificSchemas()){
				r.specificTuples.add(new Tuple(specificSchema));
			}
		} 
		return r;
	}
	
	@Override
	public void open(InputStream in) throws IOException {
		if (in instanceof DataInputStream){
			this.in = (DataInputStream)in;
		} else {
			this.in = new DataInputStream(in);
		}
	}
	
	@Override
	public DatumWrapper<ITuple> deserialize(DatumWrapper<ITuple> t) throws IOException {
		if(t == null) {
			t = new DatumWrapper<ITuple>();
		}
		if(isRollup) {
			t.swapInstances();
			this.cachedTuples.swapInstances();
		}

		ITuple tuple = (multipleSources) ? deserializeMultipleSources() : deserializeOneSource(t.datum());
		t.datum(tuple);
		
		return t;
	}
	
	private ITuple deserializeMultipleSources() throws IOException {
		CachedTuples tuples = cachedTuples.datum();
		ITuple commonTuple =tuples.commonTuple; 
		readFields(commonTuple,in);
		int schemaId = WritableUtils.readVInt(in);
		ITuple specificTuple = tuples.specificTuples.get(schemaId);
		readFields(specificTuple,in);
		ITuple result = tuples.resultTuples.get(schemaId);
		mixIntermediateIntoResult(commonTuple,specificTuple,result,schemaId);
		return result;
	}
	
	private void mixIntermediateIntoResult(ITuple commonTuple,ITuple specificTuple,ITuple result,int schemaId){
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(schemaId);
		for (int i =0 ; i < commonTranslation.length ; i++){
			int destPos = commonTranslation[i];
			result.set(destPos,commonTuple.get(i));
		}
		
		int[] specificTranslation = serInfo.getSpecificSchemaIndexTranslation(schemaId);
		for (int i =0 ; i < specificTranslation.length ; i++){
			int destPos = specificTranslation[i];
			result.set(destPos,specificTuple.get(i));
		}
	}
	
	private ITuple deserializeOneSource(ITuple reuse) throws IOException {
		CachedTuples tuples = cachedTuples.datum();
		ITuple commonTuple = tuples.commonTuple;
		readFields(commonTuple,in);
		if (reuse == null){
			reuse = tuples.resultTuples.get(0);
		}
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(0); //just one common schema
		for (int i =0 ; i < commonTranslation.length ; i++){
			int destPos = commonTranslation[i];
			reuse.set(destPos,commonTuple.get(i));
		}
		return reuse;
	}

	public void readFields(ITuple tuple, DataInput input) throws IOException {
		Schema schema = tuple.getSchema();
		for(int index = 0; index < schema.getFields().size(); index++) {
			Field field = schema.getField(index);
			switch(field.getType()){
			case INT:	tuple.set(index,WritableUtils.readVInt(input)); break;
			case LONG: tuple.set(index,WritableUtils.readVLong(input)); break;
			case DOUBLE: tuple.set(index,input.readDouble()); break;
			case FLOAT: tuple.set(index,input.readFloat()); break;
			case STRING: readUtf8(input,tuple,index); break;
			case BOOLEAN:
				byte b = input.readByte();
				tuple.set(index,(b != 0));
				break;
			case ENUM: readEnum(input,tuple,field.getObjectClass(),index); break;
			case OBJECT: readCustomObject(input,tuple,field.getObjectClass(),index); break;
			default:
				throw new IOException("Not supported type:" + field.getType());
			} 
		}
	}
	
	protected void readUtf8(DataInput input,ITuple tuple,int index) throws IOException {
		//this method is safe because tuple is internal, it's not the one received by the Reducer
		Utf8 t = (Utf8)tuple.get(index); 
		if (t == null){
			t = new Utf8();
			tuple.set(index,t);
		}
		t.readFields(input);
	}
	
	protected void readCustomObject(DataInput input,ITuple tuple,Class<?> expectedType,int index) throws IOException{
		int size = WritableUtils.readVInt(input);
		if(size >=0) {
			tmpInputBuffer.setSize(size);
			input.readFully(tmpInputBuffer.getBytes(), 0, size);
			if(tuple.get(index) == null) {
				tuple.set(index, ReflectionUtils.newInstance(expectedType, conf));
			}
			Object ob = ser.deser(tuple.get(index), tmpInputBuffer.getBytes(), 0, size);
			tuple.set(index, ob);
		} else {
			throw new IOException("Error deserializing, custom object serialized with negative length : " + size);
		}
	}
	
	protected void readEnum(DataInput input,ITuple tuple,Class<?> fieldType,int index) throws IOException{
		int ordinal = WritableUtils.readVInt(input);
		try {
			Enum<?>[] enums = cachedEnums.get(fieldType);
			if(enums == null) {
				throw new IOException("Field " + fieldType + " is not a enum type");
			}
			tuple.set(index,enums[ordinal]);
		} catch(ArrayIndexOutOfBoundsException e) {
			throw new IOException("Ordinal serialized for ");
		}
	}
	

	@Override
	public void close() throws IOException {
		in.close();
	}
}
