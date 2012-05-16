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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;

public class TupleDeserializer implements Deserializer<DatumWrapper<ITuple>> {

	private static class CachedTuples {
		private ITuple commonTuple;
		private List<ITuple> specificTuples = new ArrayList<ITuple>();
		private List<ITuple> resultTuples = new ArrayList<ITuple>();
	}

	private final TupleMRConfig tupleMRConf;
	private final SerializationInfo serInfo;
	private final boolean isRollup;
	private final boolean multipleSources;
	private DatumWrapper<CachedTuples> cachedTuples = new DatumWrapper<CachedTuples>();

	private SimpleTupleDeserializer simpleTupleDeSer;

	public TupleDeserializer(HadoopSerialization ser, TupleMRConfig tupleMRConfig, Configuration conf) {
		simpleTupleDeSer = new SimpleTupleDeserializer(ser, conf);
		this.tupleMRConf = tupleMRConfig;
		this.serInfo = tupleMRConf.getSerializationInfo();
		this.isRollup = tupleMRConf.getRollupFrom() != null && !tupleMRConf.getRollupFrom().isEmpty();
		this.multipleSources = tupleMRConf.getNumIntermediateSchemas() >= 2;
		this.cachedTuples.datum(createCachedTuples(tupleMRConf));
		this.cachedTuples.swapInstances(); // do rollup
		this.cachedTuples.datum(createCachedTuples(tupleMRConf));
	}

	private static CachedTuples createCachedTuples(TupleMRConfig config) {
		SerializationInfo serInfo = config.getSerializationInfo();
		boolean multipleSources = config.getNumIntermediateSchemas() >= 2;
		CachedTuples r = new CachedTuples();
		r.commonTuple = new Tuple(serInfo.getCommonSchema());
		for(Schema sourceSchema : config.getIntermediateSchemas()) {
			r.resultTuples.add(new Tuple(sourceSchema));
		}

		if(multipleSources) {
			for(Schema specificSchema : serInfo.getSpecificSchemas()) {
				r.specificTuples.add(new Tuple(specificSchema));
			}
		}
		return r;
	}

	@Override
	public void open(InputStream in) throws IOException {
		simpleTupleDeSer.open(in);
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
		ITuple commonTuple = tuples.commonTuple;

		simpleTupleDeSer.readFields(commonTuple, serInfo.getCommonSchemaDeserializers());
		int schemaId = WritableUtils.readVInt(simpleTupleDeSer.getInput());
		ITuple specificTuple = tuples.specificTuples.get(schemaId);
		simpleTupleDeSer.readFields(specificTuple, serInfo.getSpecificSchemaDeserializers().get(schemaId));
		ITuple result = tuples.resultTuples.get(schemaId);
		mixIntermediateIntoResult(commonTuple, specificTuple, result, schemaId);
		return result;
	}

	private void mixIntermediateIntoResult(ITuple commonTuple, ITuple specificTuple, ITuple result, int schemaId) {
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(schemaId);
		for(int i = 0; i < commonTranslation.length; i++) {
			int destPos = commonTranslation[i];
			result.set(destPos, commonTuple.get(i));
		}

		int[] specificTranslation = serInfo.getSpecificSchemaIndexTranslation(schemaId);
		for(int i = 0; i < specificTranslation.length; i++) {
			int destPos = specificTranslation[i];
			result.set(destPos, specificTuple.get(i));
		}
	}

	private ITuple deserializeOneSource(ITuple reuse) throws IOException {
		CachedTuples tuples = cachedTuples.datum();
		ITuple commonTuple = tuples.commonTuple;
		simpleTupleDeSer.readFields(commonTuple, serInfo.getCommonSchemaDeserializers());
		if(reuse == null) {
			reuse = tuples.resultTuples.get(0);
		}
		int[] commonTranslation = serInfo.getCommonSchemaIndexTranslation(0); // just one common schema
		for(int i = 0; i < commonTranslation.length; i++) {
			int destPos = commonTranslation[i];
			reuse.set(destPos, commonTuple.get(i));
		}
		return reuse;
	}

	@Override
	public void close() throws IOException {
		simpleTupleDeSer.close();
	}
}
