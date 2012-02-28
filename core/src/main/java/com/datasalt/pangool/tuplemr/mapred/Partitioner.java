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
package com.datasalt.pangool.tuplemr.mapred;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRException;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<DatumWrapper<ITuple>, NullWritable> implements Configurable {

	private TupleMRConfig grouperConfig;
	private SerializationInfo serInfo;
	
	private Configuration conf;
	private final Utf8 HELPER_UTF8 = new Utf8(); //to perform hashCode of strings
	
	@Override
	public int getPartition(DatumWrapper<ITuple> key, NullWritable value, int numPartitions) {
		if(numPartitions == 1) {
			//in this case the schema is not checked if it's valid
			return 0;
		} else {
			ITuple tuple = key.datum();
			String sourceName = tuple.getSchema().getName();
			Integer schemaId = grouperConfig.getSchemaIdByName(sourceName);
			if(schemaId == null) {
				throw new RuntimeException("Schema name '" + sourceName + "' is unknown. Known schemas are : "
				    + grouperConfig.getIntermediateSchemaNames());
			}
			int[] fieldsToPartition = serInfo.getPartitionFieldsIndexes().get(schemaId);
			return (partialHashCode(tuple, fieldsToPartition) & Integer.MAX_VALUE) % numPartitions;
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if(conf != null) {
			this.conf = conf;
			try {
				this.grouperConfig = TupleMRConfig.get(conf);
				this.serInfo = grouperConfig.getSerializationInfo();
			} catch (TupleMRException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	/**
	 * Calculates a combinated hashCode using the specified number of fields.
	 * 
	 */
	public int partialHashCode(ITuple tuple,int[] fields) {
		int result = 0;
		for(int field : fields) {
			Object o = tuple.get(field);
			int hashCode;
			if (o instanceof Utf8){ //since String.hashCode() != Utf8.hashCode()
				HELPER_UTF8.set((String)o); 
				hashCode = HELPER_UTF8.hashCode();
			} else if (o instanceof Text){
				HELPER_UTF8.set((Text) o);
				hashCode = HELPER_UTF8.hashCode();
			} else {
				hashCode = o.hashCode();
			}
			result = result * 31 + hashCode;
		}
		return result ;
	}
	
}