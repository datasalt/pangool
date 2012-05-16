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
import java.io.OutputStream;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;

public class TupleSerializer implements Serializer<DatumWrapper<ITuple>> {

	private final TupleMRConfig tupleMRConfig;
	private boolean isMultipleSources = false;
	private final SerializationInfo serInfo;
	private final Schema commonSchema;
	
	// Makes use of an "agnostic" simple Tuple serializer for serializing Tuples
	// Enable code reusing
	private final SimpleTupleSerializer tupleSerializer;

	public TupleSerializer(HadoopSerialization ser, TupleMRConfig tupleMRConfig) {
		tupleSerializer = new SimpleTupleSerializer(ser);
		this.tupleMRConfig = tupleMRConfig;
		this.serInfo = tupleMRConfig.getSerializationInfo();
		this.commonSchema = this.serInfo.getCommonSchema();
		this.isMultipleSources = (tupleMRConfig.getNumIntermediateSchemas() >= 2);
	}

	public void open(OutputStream out) {
		tupleSerializer.open(out);
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
		// Tuple schema is not checked here
		tupleSerializer.write(commonSchema, tuple, commonTranslation, serInfo.getCommonSchemaSerializers());
	}

	private void multipleSourcesSerialization(ITuple tuple) throws IOException {
		String sourceName = tuple.getSchema().getName();
		Integer schemaId = tupleMRConfig.getSchemaIdByName(sourceName);
		if (schemaId == null){
			throw new IOException("Schema '" + tuple.getSchema() +"' is not a valid intermediate schema");
		}
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(schemaId);
		// Serialize common
		tupleSerializer.write(commonSchema, tuple, commonTranslation, serInfo.getCommonSchemaSerializers());
		// Serialize source id
		WritableUtils.writeVInt(tupleSerializer.getOut(), schemaId);
		// Serialize rest of the fields
		Schema specificSchema = serInfo.getSpecificSchema(schemaId);
		int[] specificTranslation = serInfo
				.getSpecificSchemaIndexTranslation(schemaId);
		tupleSerializer.write(specificSchema, tuple, specificTranslation, serInfo.getSpecificSchemaSerializers().get(schemaId));
	}

	public void close() throws IOException {
		tupleSerializer.close();
	}
}
