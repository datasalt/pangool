/**
 * Copyright [2011] [Datasalt Systems S.L.]
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
package com.datasalt.pangolin.grouper.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.datasalt.pangolin.grouper.io.BaseTuple.InvalidFieldException;

/**
 * 
 * @author epalace
 *
 */
public class TuplePartitioner extends Partitioner<Tuple,NullWritable> implements Configurable{

	public static final String CONF_PARTITIONER_FIELDS ="datasalt.grouper.partitioner_fields";
	
	private Configuration conf;
	//private FieldsDescription schema;
	private String[] groupFields;
	
	@Override
	public int getPartition(Tuple key, NullWritable value, int numPartitions) {
		try{
		int result =  key.partialHashCode(groupFields) % numPartitions;
		return result;
		} catch(InvalidFieldException e){
			throw new RuntimeException(e);
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		//try{
		if (conf != null){
			this.conf = conf;
			//this.schema = FieldsDescription.parse(conf);
		}
		
		String fieldsGroupStr = conf.get(CONF_PARTITIONER_FIELDS);
		//TODO what to do here if null
		groupFields = fieldsGroupStr.split(",");
		
//		} catch(GrouperException e){
//			throw new RuntimeException(e);
//		}
	}
}
