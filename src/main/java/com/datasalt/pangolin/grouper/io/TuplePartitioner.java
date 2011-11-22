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
import org.apache.log4j.Logger;

import com.datasalt.pangolin.grouper.Constants;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;

/**
 * 
 * @author epalace
 *
 */
public class TuplePartitioner extends Partitioner<Tuple,NullWritable> implements Configurable{

	private Configuration conf;
	private Schema schema;
	private int[] groupFieldsIndexes;
	
	@Override
	public int getPartition(Tuple key, NullWritable value, int numPartitions) {
		int result =  key.partialHashCode(groupFieldsIndexes) % numPartitions;
		return result;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		try{
		if (conf != null){
			this.conf = conf;
			this.schema = Schema.parse(conf);
		}
		
		String fieldsGroupStr = conf.get(Constants.CONF_MIN_GROUP);
		String[] fieldsGroup = fieldsGroupStr.split(",");
		groupFieldsIndexes = new int[fieldsGroup.length];
		for (int i=0 ; i < fieldsGroup.length;i++){
			groupFieldsIndexes[i] = schema.getIndexByFieldName(fieldsGroup[i]);
		}
		} catch(GrouperException e){
			throw new RuntimeException(e);
		}
	}
}
