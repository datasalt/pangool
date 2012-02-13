package com.datasalt.pangool.io.tuple.ser;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SerializationInfo;
import com.datasalt.pangool.io.Serialization;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ITuple;

public class PangoolSerializer implements Serializer<DatumWrapper<ITuple>> {

	private final Serialization ser;
	
	private DataOutputStream out;
	private final CoGrouperConfig coGrouperConfig;
	private final Text HELPER_TEXT = new Text();
	private static final Text EMPTY_TEXT = new Text("");
	private boolean isMultipleSources=false;
	private final DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();
	private final SerializationInfo serInfo;
	private final Schema commonSchema;
	
	public PangoolSerializer(Serialization ser,CoGrouperConfig grouperConfig) {
		this.ser = ser;
		this.coGrouperConfig = grouperConfig;
		this.serInfo = grouperConfig.getSerializationInfo();
		this.commonSchema = this.serInfo.getCommonSchema();
		this.isMultipleSources = (coGrouperConfig.getNumSources() >= 2);
	}

	public void open(OutputStream out) {
		if (out instanceof DataOutputStream){
			this.out = (DataOutputStream) out;
		} else {
			this.out = new DataOutputStream(out);
		}
	}

	public void serialize(DatumWrapper<ITuple> wrapper) throws IOException {
		ITuple tuple = wrapper.currentDatum();
		//TODO check that schema is valid
		if (isMultipleSources){
			multipleSourcesSerialization(tuple);
		} else {
			oneSourceSerialization(tuple);
		}
	}
	
	private void oneSourceSerialization(ITuple tuple) throws IOException {
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(0);
		write(commonSchema,tuple,commonTranslation,out);
	}
	
	private void multipleSourcesSerialization(ITuple tuple) throws IOException {
		String sourceName = tuple.getSchema().getName();
		int sourceId = coGrouperConfig.getSourceIdByName(sourceName);
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(sourceId); 
		//serialize common 
		write(commonSchema,tuple,commonTranslation,out);
		//serialize source id
		WritableUtils.writeVInt(out, sourceId);
		//serialize rest of the fields
		Schema specificSchema = serInfo.getSpecificSchema(sourceId);
		int[] specificTranslation =serInfo.getSpecificSchemaIndexTranslation(sourceId);
		write(specificSchema,tuple,specificTranslation,out);
	}

	public void close() throws IOException {
		this.out.close();
	}

	/**
	 * 
	 *  The size of the translation table matches the destinationSchema fields size.
	 *  
	 * @param destinationSchema
	 * @param tuple
	 * @param translationTable If null then no translation is performed
	 * @param output
	 * @throws IOException
	 */
	private void write(Schema destinationSchema, ITuple tuple,int[] translationTable, DataOutput output) throws IOException {
		for(int i=0; i < destinationSchema.getFields().size(); i++) {
			Field field = destinationSchema.getField(i);
			String fieldName = field.name();
			Class<?> fieldType = field.getType();
			Object element = tuple.get(translationTable[i]);
			try {
				if(fieldType == VIntWritable.class) {
					WritableUtils.writeVInt(output, (Integer) element);
				} else if(fieldType == VLongWritable.class) {
					WritableUtils.writeVLong(output, (Long) element);
				} else if(fieldType == Integer.class) {
					output.writeInt((Integer) element);
				} else if(fieldType == Long.class) {
					output.writeLong((Long) element);
				} else if(fieldType == Double.class) {
					output.writeDouble((Double) element);
				} else if(fieldType == Float.class) {
					output.writeFloat((Float) element);
				} else if(fieldType == String.class) {
					if (element == null){
						EMPTY_TEXT.write(output);
					} else if (element instanceof Text){
						((Text)element).write(output);
					} else if (element instanceof String){
						HELPER_TEXT.set((String)element);
						HELPER_TEXT.write(output);
					} 
				} else if(fieldType == Boolean.class) {
					output.write((Boolean) element ? 1 : 0);
				} else if(fieldType.isEnum()) {
					writeEnum((Enum<?>)element,fieldType,fieldName,output);
				} else {
					writeCustomObject(element,output);
				}
			} catch(ClassCastException e) {
				raiseClassCastException(fieldName, element, fieldType);
			} // end for
		} 
		
	}
	
	private void writeCustomObject(Object element, DataOutput output) throws IOException{
		if(element == null) {
			WritableUtils.writeVInt(output, 0);
		} else {
			tmpOutputBuffer.reset();
			ser.ser(element, tmpOutputBuffer);
			//the length of the object is prepended
			WritableUtils.writeVInt(output, tmpOutputBuffer.getLength());
			output.write(tmpOutputBuffer.getData(), 0, tmpOutputBuffer.getLength());
		}
	}
	
	private void writeEnum(Enum<?> element,Class<?> expectedType,String fieldName,DataOutput output) throws IOException{
		Enum<?> e = (Enum<?>) element;
		if(e.getClass() != expectedType) {
			throw new IOException("Field '" + fieldName + "' contains '" + element + "' which is "
			    + element.getClass().getName() + ".The expected type is " + expectedType.getName());
		}
		WritableUtils.writeVInt(output, e.ordinal());
	}
	
	private void raiseClassCastException(String fieldName,Object element,Class<?> expectedType) throws IOException {
		throw new IOException("Field '" + fieldName + "' contains '" + element + "' which is "
		    + element.getClass().getName() + ".The expected type is " + expectedType.getName());
	}
	
}
