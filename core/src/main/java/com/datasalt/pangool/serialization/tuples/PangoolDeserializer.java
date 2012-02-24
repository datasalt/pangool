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
package com.datasalt.pangool.serialization.tuples;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.cogroup.TupleMRConfig;
import com.datasalt.pangool.cogroup.SerializationInfo;
import com.datasalt.pangool.io.Buffer;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.serialization.hadoop.HadoopSerialization;


public class PangoolDeserializer implements Deserializer<DatumWrapper<ITuple>> {

	private static class CachedTuples {
		private ITuple commonTuple;
		private List<ITuple> specificTuples=new ArrayList<ITuple>();
		private List<ITuple> resultTuples=new ArrayList<ITuple>();
	}
	
	
	private final TupleMRConfig coGrouperConf;
	private final Configuration conf;
	private final SerializationInfo serInfo;
	private  DataInputStream in;
	private final HadoopSerialization ser;
	private final boolean isRollup;
	private final boolean multipleSources;
	private final Map<Class<?>, Enum<?>[]> cachedEnums;

	private final Buffer tmpInputBuffer = new Buffer();
	private DatumWrapper<CachedTuples> cachedTuples = new DatumWrapper<CachedTuples>();

	public PangoolDeserializer(HadoopSerialization ser, TupleMRConfig grouperConfig, Configuration conf) {
		this.coGrouperConf = grouperConfig;
		this.conf = conf;
		this.serInfo = coGrouperConf.getSerializationInfo();
		this.ser = ser;
		this.cachedEnums = PangoolSerialization.getEnums(grouperConfig);
		this.isRollup = coGrouperConf.getRollupFrom() != null && !coGrouperConf.getRollupFrom().isEmpty();
		this.multipleSources = coGrouperConf.getNumIntermediateSchemas() >= 2;
		this.cachedTuples.datum(createCachedTuples(coGrouperConf));
		this.cachedTuples.swapInstances(); //do rollup
		this.cachedTuples.datum(createCachedTuples(coGrouperConf));
		
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
		int sourceId = WritableUtils.readVInt(in);
		ITuple specificTuple = tuples.specificTuples.get(sourceId);
		readFields(specificTuple,in);
		ITuple result = tuples.resultTuples.get(sourceId);
		mixIntermediateIntoResult(commonTuple,specificTuple,result,sourceId);
		return result;
	}
	
	private void mixIntermediateIntoResult(ITuple commonTuple,ITuple specificTuple,ITuple result,int sourceId){
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(sourceId);
		for (int i =0 ; i < commonTranslation.length ; i++){
			int destPos = commonTranslation[i];
			result.set(destPos,commonTuple.get(i));
		}
		
		int[] specificTranslation = serInfo.getSpecificSchemaIndexTranslation(sourceId);
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
			Class<?> fieldType = schema.getField(index).getType();
			if(fieldType == VIntWritable.class) {
				tuple.set(index,WritableUtils.readVInt(input));
			} else if(fieldType == VLongWritable.class) {
				tuple.set(index,WritableUtils.readVLong(input));
			} else if(fieldType == Integer.class) {
				tuple.set(index,input.readInt());
			} else if(fieldType == Long.class) {
				tuple.set(index,input.readLong());
			} else if(fieldType == Double.class) {
				tuple.set(index,input.readDouble());
			} else if(fieldType == Float.class) {
				tuple.set(index,input.readFloat());
			} else if(fieldType == String.class) {
				readText(input,tuple,index);
			} else if(fieldType == Boolean.class) {
				byte b = input.readByte();
				tuple.set(index,(b != 0));
			} else if(fieldType.isEnum()) {
				readEnum(input,tuple,fieldType,index);
			} else {
				readCustomObject(input,tuple,fieldType,index);
			} // end for
		}
	}
	
	protected void readText(DataInput input,ITuple tuple,int index) throws IOException {
		Text t = (Text)tuple.get(index);
		if (t == null){
			t = new Text();
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
			tuple.set(index,null); 
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
