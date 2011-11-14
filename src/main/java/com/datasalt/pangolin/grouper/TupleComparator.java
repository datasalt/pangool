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
package com.datasalt.pangolin.grouper;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import com.datasalt.pangolin.grouper.Schema.Field;
import com.datasalt.pangolin.grouper.Schema.Field.SortCriteria;

public class TupleComparator extends WritableComparator implements Configurable{

	private Configuration conf;
	private Schema schema;
	
	protected TupleComparator(Class<? extends WritableComparable> keyClass) {
		super(Tuple.class);
	}
	
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			int length1 = readVInt(b1, s1);
			int length2 = readVInt(b2, s2);
			int offset1 = WritableUtils.decodeVIntSize(b1[s1])+s1;
			int offset2 = WritableUtils.decodeVIntSize(b2[s2])+s2;
			for (Field field : schema.getFields()){
				Class type = field.getType();
				SortCriteria sort = field.getSortCriteria();
				if (type == Integer.class){
					int value1 = readInt(b1,offset1);
					int value2 = readInt(b2,offset2);
					if (value1 > value2){
						return (sort == SortCriteria.ASC) ? 1 : -1;
					} else if (value1 < value2 ){
						return (sort == SortCriteria.ASC) ? -1 : 1;
					}
					offset1 += Integer.SIZE; 
					offset2 += Integer.SIZE; 
				} else if (type == Long.class){
					long value1 = readLong(b1,offset1);
					long value2 = readLong(b2,offset2);
					if (value1 > value2){
						return (sort == SortCriteria.ASC) ? 1 : -1;
					} else if (value1 < value2 ){
						return (sort == SortCriteria.ASC) ? -1 : 1;
					}
					offset1 += Long.SIZE; 
					offset2 += Long.SIZE; 
				} else if (type == VIntWritable.class){
					int value1 = readVInt(b1,offset1);
					int value2 = readVInt(b2,offset2);
					if (value1 > value2){
						return (sort == SortCriteria.ASC) ? 1 : -1;
					} else if (value1 < value2 ){
						return (sort == SortCriteria.ASC) ? -1 : 1;
					}
					offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
					offset2 += WritableUtils.decodeVIntSize(b2[offset2]); 
					
				} else if (type == VLongWritable.class){
					long value1 = readVLong(b1,offset1);
					long value2 = readVLong(b2,offset2);
					if (value1 > value2){
						return (sort == SortCriteria.ASC) ? 1 : -1;
					} else if (value1 < value2 ){
						return (sort == SortCriteria.ASC) ? -1 : 1;
					}
					offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
					offset2 += WritableUtils.decodeVIntSize(b2[offset2]); 
				} else if (type == Float.class){
					float value1 = readFloat(b1,offset1);
					float value2 = readFloat(b2,offset2);
					if (value1 > value2){
						return (sort == SortCriteria.ASC) ? 1 : -1;
					} else if (value1 < value2 ){
						return (sort == SortCriteria.ASC) ? -1 : 1;
					}
					offset1 += Float.SIZE;
					offset2 += Float.SIZE; 
					
				} else if (type == String.class){
					int strLength1 = readVInt(b1,offset1);
					int strLength2 = readVInt(b2,offset2);
					int comparison = compareBytes(b1,offset1,strLength1,b2,offset2,strLength2);
					if (comparison != 0){
						return (sort == SortCriteria.ASC) ? comparison : (-1*comparison);
					}
					offset1 += strLength1;
					offset2 += strLength2;
				}
			}
			return 0; //equals
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if (conf != null){
			this.conf = conf;
			this.schema = Grouper.getSchema(conf);
		}
	}
}
