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
package com.datasalt.pangolin.grouper.io.tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.FieldsDescription.Field;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.SortCriteria;
import com.datasalt.pangolin.grouper.SortCriteria.SortElement;
import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;

/**
 * 
 * Binary comparator for {@link ITuple} objects.
 * 
 * @author eric
 * 
 */
public class SortComparator extends WritableComparator implements Configurable {

	private Configuration conf;
	private FieldsDescription schema;
	private SortCriteria sortCriteria;
	@SuppressWarnings("rawtypes")
  private Map<Class,RawComparator> instancedComparators;

	public SortComparator() {
		super(Tuple.class);
	}

	public SortCriteria getSortCriteria(){
		return sortCriteria;
	}
	
	
	@Override
	public int compare(Object w1,Object w2) {
		return compare((WritableComparable)w1,(WritableComparable)w2);
	}
	
	@Override
	public int compare(WritableComparable w1,WritableComparable w2) {
		int fieldsCompared = sortCriteria.getSortElements().length;
		return compare(fieldsCompared,w1,w2);
	}
	
	@SuppressWarnings("unchecked")
  public int compare(int maxFieldsCompared,Object w1,Object w2){
		try {
			ITuple tuple1 = (ITuple) w1;
			ITuple tuple2 = (ITuple) w2;
			for(int depth = 0; depth < maxFieldsCompared; depth++) {
				Field field = schema.getField(depth);
				String fieldName = field.getName();
				SortElement sortElement = sortCriteria.getSortElementByFieldName(field.getName());
				SortOrder sort = SortOrder.ASCENDING; // by default
				@SuppressWarnings("rawtypes")
        RawComparator comparator = null;
				if(sortElement != null) {
					sort = sortElement.getSortOrder();
					comparator = instancedComparators.get(sortElement.getComparator());
				} else {
					throw new RuntimeException("Fatal error : Trying to sort by field '" + fieldName + "' but not present in sortCriteria:" + sortCriteria);
				}
				int comparison;
				if (comparator != null){
					comparison = comparator.compare(tuple1.getObject(fieldName), tuple2.getObject(fieldName));
				} else {
				 comparison = compareObjects(tuple1.getObject(fieldName), tuple2.getObject(fieldName));
				}
				if(comparison != 0) {
					return (sort == SortOrder.ASCENDING) ? comparison : -comparison;
				}
			}
			return 0;
		} catch(InvalidFieldException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Compares two objects 
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
  public static int compareObjects(Object element1, Object element2 ){

		if (element1 == null){
			return (element2 == null) ? 0 : -1;
		} else if (element2 == null){
			return 1;
		} else  {
			if (element1 instanceof Comparable){
				return ((Comparable) element1).compareTo(element2);
			} else if (element2 instanceof Comparable){
				return -((Comparable)element2).compareTo(element1);
				
			} else {
				//both objects are not Comparable
				//TODO is this correct? 
				return 0;
			}
		}
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
	 * Compares {@link ITuple} objects serialized in binary up to a maximum depth specified in <b>maxFieldsCompared</b> 
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
				} else if(type == VIntWritable.class || type.isEnum()) {
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
					Boolean boolean1 = (value1 == 0) ? false : true;
					Boolean boolean2 = (value2 == 0) ? false : true;
					int comparison = boolean1.compareTo(boolean2);
					if (comparison != 0){
						return (sort == SortOrder.ASCENDING) ? comparison : -comparison;
					}
					/*
					if(value1 > value2) {
						return (sort == SortOrder.ASCENDING) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASCENDING) ? -1 : 1;
					}*/
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
		WritableComparator.define(Tuple.class, new SortComparator());
	}
}
