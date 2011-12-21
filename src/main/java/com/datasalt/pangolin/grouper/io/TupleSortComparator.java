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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.FieldsDescription.Field;
import com.datasalt.pangolin.grouper.SortCriteria;
import com.datasalt.pangolin.grouper.SortCriteria.SortElement;
import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;

/**
 * 
 * Binary comparator for {@link Tuple} objects.
 * 
 * @author epalace
 * 
 */
public class TupleSortComparator extends WritableComparator implements Configurable {

	private Configuration conf;
	private FieldsDescription schema;
	private SortCriteria sortCriteria;
	private Map<Class,RawComparator> instancedComparators;

	public TupleSortComparator() {
		super(Tuple.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int fieldsCompared = sortCriteria.getSortElements().length;
		return compare(fieldsCompared, b1, s1, l1, b2, s2, l2);
	}

	private void setSchema(FieldsDescription schema){
		this.schema = schema;
	}
	
	private void setSortCriteria(SortCriteria sortCriteria) throws GrouperException{
		if (this.schema == null){
			throw new GrouperException("Schema not set");
		}
		
		this.sortCriteria = sortCriteria;
		this.instancedComparators = new HashMap<Class,RawComparator>();
		for(SortElement sortElement : this.sortCriteria.getSortElements()){
			Class<? extends RawComparator> clazz = sortElement.getComparator();
			if (clazz != null){
				RawComparator comparator = ReflectionUtils.newInstance(clazz, conf); 
				instancedComparators.put(clazz,comparator);
			}
		}
	}
	
	
	/**
	 * Compares {@link TupleImpl} objects serialized in binary up to a maximum depth specified in <b>maxFieldsCompared</b> 
	 * 
	 * @param maxFieldsCompared
	 * 
	 */
	public int compare(int maxFieldsCompared, byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		try {
			int offset1 = s1;
			int offset2 = s2;
			for(int depth = 0; depth < maxFieldsCompared; depth++) {
				Field field = schema.getFields()[depth];
				Class<?> type = field.getType();
				SortElement sortElement = sortCriteria.getSortElementByFieldName(field.getName());
				SortOrder sort=SortOrder.ASCENDING; //by default
				RawComparator<?> comparator=null;
				if(sortElement != null){
					sort=sortElement.getSortOrder();
					comparator = instancedComparators.get(sortElement.getComparator());
				} 
				if (comparator != null){
				//The rest of types using compareBytes
					int length1 = readVInt(b1, offset1);
					int length2 = readVInt(b2, offset2);
					offset1+=WritableUtils.decodeVIntSize(b1[offset1]);
					offset2+=WritableUtils.decodeVIntSize(b2[offset2]);
					int comparison = comparator.compare(b1, offset1,length1,b2,offset2,length2);
					if (comparison != 0){
						return (sort == SortOrder.ASCENDING) ? comparison : -comparison;
					}
					offset1 += length1;
					offset2 += length2;
				} else if(type == Integer.class) {
					int value1 = readInt(b1, offset1);
					int value2 = readInt(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASCENDING) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASCENDING) ? -1 : 1;
					}
					offset1 += Integer.SIZE / 8;
					offset2 += Integer.SIZE / 8;
				} else if(type == Long.class) {
					long value1 = readLong(b1, offset1);
					long value2 = readLong(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASCENDING) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASCENDING) ? -1 : 1;
					}
					offset1 += Long.SIZE / 8;
					offset2 += Long.SIZE / 8;
				} else if(type == VIntWritable.class) {
					int value1 = readVInt(b1, offset1);
					int value2 = readVInt(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASCENDING) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASCENDING) ? -1 : 1;
					}
					int vintSize =WritableUtils.decodeVIntSize(b1[offset1]);
					offset1 += vintSize;
					offset2 += vintSize;

				} else if(type == VLongWritable.class) {
					long value1 = readVLong(b1, offset1);
					long value2 = readVLong(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASCENDING) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASCENDING) ? -1 : 1;
					}
					int vIntSize =WritableUtils.decodeVIntSize(b1[offset1]); 
					offset1 += vIntSize;
					offset2 += vIntSize;
				} else if(type == Float.class) {
					float value1 = readFloat(b1, offset1);
					float value2 = readFloat(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASCENDING) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASCENDING) ? -1 : 1;
					}
					offset1 += Float.SIZE / 8;
					offset2 += Float.SIZE / 8;

				} else if(type == Boolean.class) {
					byte value1 = b1[offset1++];
					byte value2 = b2[offset2++];
					if(value1 > value2) {
						return (sort == SortOrder.ASCENDING) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASCENDING) ? -1 : 1;
					}
				} else {
					//String(Text) and the rest of types using compareBytes
					int length1 = readVInt(b1, offset1);
					int length2 = readVInt(b2, offset2);
					offset1+=WritableUtils.decodeVIntSize(b1[offset1]);
					offset2+=WritableUtils.decodeVIntSize(b2[offset2]);
					int comparison = compareBytes(b1, offset1, length1, b2, offset2, length2);
					if(comparison != 0) {
						return (sort == SortOrder.ASCENDING) ? comparison : (-comparison);
					}
					offset1 += length1;
					offset2 += length2;
					
				}
			}
			return 0; // equals
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		try {
			if(conf != null) {
				this.conf = conf;
				setSchema(FieldsDescription.parse(conf));
				try {
					setSortCriteria(SortCriteria.parse(conf));
				} catch(GrouperException e) {
					throw new RuntimeException(e);
				}
			}
		} catch(GrouperException e) {
			throw new RuntimeException(e);
		}
	}
	
	static {
		// statically added in register
		WritableComparator.define(Tuple.class, new TupleSortComparator());
	}
}
