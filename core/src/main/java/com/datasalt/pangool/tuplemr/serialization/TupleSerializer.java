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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;

public class TupleSerializer implements Serializer<DatumWrapper<ITuple>> {

	private final HadoopSerialization ser;

	private DataOutputStream out;
	private final TupleMRConfig tupleMRConfig;
	private final Utf8 HELPER_TEXT = new Utf8();
	private boolean isMultipleSources = false;
	private final DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();
	private final SerializationInfo serInfo;
	private final Schema commonSchema;

	public TupleSerializer(HadoopSerialization ser, TupleMRConfig tupleMRConfig) {
		this.ser = ser;
		this.tupleMRConfig = tupleMRConfig;
		this.serInfo = tupleMRConfig.getSerializationInfo();
		this.commonSchema = this.serInfo.getCommonSchema();
		this.isMultipleSources = (tupleMRConfig.getNumIntermediateSchemas() >= 2);
	}

	public void open(OutputStream out) {
		if (out instanceof DataOutputStream) {
			this.out = (DataOutputStream) out;
		} else {
			this.out = new DataOutputStream(out);
		}
	}

	public void serialize(DatumWrapper<ITuple> wrapper) throws IOException {
		ITuple tuple = wrapper.datum();
		if (isMultipleSources) {
			multipleSourcesSerialization(tuple);
		} else {
			oneSourceSerialization(tuple);
		}
	}

	private void oneSourceSerialization(ITuple tuple) throws IOException {
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(0);
		//tuple schema is not checked here
		write(commonSchema, tuple, commonTranslation, out);
	}

	private void multipleSourcesSerialization(ITuple tuple) throws IOException {
		String sourceName = tuple.getSchema().getName();
		Integer schemaId = tupleMRConfig.getSchemaIdByName(sourceName);
		if (schemaId == null){
			throw new IOException("Schema '" + tuple.getSchema() +"' is not a valid intermediate schema");
		}
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(schemaId);
		// serialize common
		write(commonSchema, tuple, commonTranslation, out);
		// serialize source id
		WritableUtils.writeVInt(out, schemaId);
		// serialize rest of the fields
		Schema specificSchema = serInfo.getSpecificSchema(schemaId);
		int[] specificTranslation = serInfo
				.getSpecificSchemaIndexTranslation(schemaId);
		write(specificSchema, tuple, specificTranslation, out);
	}

	public void close() throws IOException {
		this.out.close();
	}

	/**
	 * 
	 * The size of the translation table matches the destinationSchema fields
	 * size.
	 * 
	 * @param destinationSchema
	 * @param tuple
	 * @param translationTable
	 *          If null then no translation is performed
	 * @param output
	 * @throws IOException
	 */
	private void write(Schema destinationSchema, ITuple tuple,
			int[] translationTable, DataOutput output) throws IOException {
		for (int i = 0; i < destinationSchema.getFields().size(); i++) {
			Field field = destinationSchema.getField(i);
			Type fieldType = field.getType();
			Object element = tuple.get(translationTable[i]);
			try {
				switch(fieldType){
				case INT:
					WritableUtils.writeVInt(output, (Integer) element); break;
				case LONG:
					WritableUtils.writeVLong(output, (Long) element);break;
				case DOUBLE:
					output.writeDouble((Double) element); break;
				case FLOAT:
					output.writeFloat((Float) element);break;
				case STRING:
					if (element instanceof Text){
						((Text)element).write(output);
					} else if (element instanceof String){
						HELPER_TEXT.set((String)element);
						HELPER_TEXT.write(output);
					} else {
						raiseClassCastException(null,field,element);
					}
					break;
				case BOOLEAN:
					output.write((Boolean) element ? 1 : 0); break;
				case ENUM:
					writeEnum((Enum<?>) element, field, output); break;
				case OBJECT:
					writeCustomObject(element,output); break;
				default:
					throw new IOException("Not supported type:" + fieldType);
				}
			} catch(ClassCastException e) {
				raiseClassCastException(e, field,element);
			} catch(NullPointerException e) {
				raiseNullInstanceException(e, field, element);
			}
		} // end for		
	}
	
	private void writeCustomObject(Object element, DataOutput output) throws IOException{
			tmpOutputBuffer.reset();
			ser.ser(element, tmpOutputBuffer);
			WritableUtils.writeVInt(output, tmpOutputBuffer.getLength());
			output.write(tmpOutputBuffer.getData(), 0, tmpOutputBuffer.getLength());
	}

	private void writeEnum(Enum<?> element, Field field, DataOutput output) throws IOException {
		Enum<?> e = (Enum<?>) element;
		Class<?> expectedType = field.getObjectClass();
		if (e.getClass() != expectedType) {
			throw new IOException("Field '" + field.getName() + "' contains '" + element
					+ "' which is " + element.getClass().getName()
					+ ".The expected type is " + expectedType.getName());
		}
		WritableUtils.writeVInt(output, e.ordinal());
	}
	
	private void raiseClassCastException(ClassCastException cause,Field field,Object element) throws IOException {
		throw new IOException("Field '" + field.getName() + "' with type: '" + field.getType() + "' can't contain '" + element + "' which is "
		    + element.getClass().getName(),cause);
	}
	
	private void raiseNullInstanceException(NullPointerException cause, Field field,Object element) throws IOException {
		throw new IOException("Field '" + field.getName() + "' with type " + field.getType() +" can't contain null value",cause);
	}	
}
