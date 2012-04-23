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

import static org.apache.hadoop.io.WritableComparator.compareBytes;
import static org.apache.hadoop.io.WritableComparator.readDouble;
import static org.apache.hadoop.io.WritableComparator.readFloat;
import static org.apache.hadoop.io.WritableComparator.readVInt;
import static org.apache.hadoop.io.WritableComparator.readVLong;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.tuplemr.Criteria;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.serialization.TupleSerialization;

/**
 * Tuple-based MapRed job binary comparator. It decodes the binary serialization
 * performed by {@link TupleSerialization}.
 * 
 * Used to group tuples according to
 * {@link TupleMRConfigBuilder#setOrderBy(com.datasalt.pangool.tuplemr.OrderBy)}
 */
@SuppressWarnings("rawtypes")
public class SortComparator implements RawComparator<ITuple>, Configurable {

	protected Configuration conf;
	protected TupleMRConfig tupleMRConf;
	protected SerializationInfo serInfo;

	protected final SerializerComparator serializerComparator = new SerializerComparator();

	private static final class Offsets {
		protected int offset1 = 0;
		protected int offset2 = 0;
	}

	protected Offsets offsets = new Offsets();
	protected boolean isMultipleSources;

	public TupleMRConfig getConfig() {
		return tupleMRConf;
	}

	public SortComparator() {
	}

	/**
	 * Never called in MapRed jobs. Just for completion and test purposes
	 */
	@Override
	public int compare(ITuple w1, ITuple w2) {
		if(isMultipleSources) {
			int schemaId1 = tupleMRConf.getSchemaIdByName(w1.getSchema().getName());
			int schemaId2 = tupleMRConf.getSchemaIdByName(w2.getSchema().getName());
			int[] indexes1 = serInfo.getCommonSchemaIndexTranslation(schemaId1);
			int[] indexes2 = serInfo.getCommonSchemaIndexTranslation(schemaId2);
			Criteria c = tupleMRConf.getCommonCriteria();
			int comparison = compare(serInfo.getCommonSchema(), c, w1, indexes1, w2, indexes2,serInfo.getCommonSchemaSerializers());
			if(comparison != 0) {
				return comparison;
			} else if(schemaId1 != schemaId2) {
				int r = schemaId1 - schemaId2;
				return (tupleMRConf.getSchemasOrder() == Order.ASC) ? r : -r;
			}
			int schemaId = schemaId1;
			c = tupleMRConf.getSpecificOrderBys().get(schemaId);
			if(c != null) {
				int[] indexes = serInfo.getSpecificSchemaIndexTranslation(schemaId);
				return compare(serInfo.getSpecificSchema(schemaId), c, w1, indexes, w2, indexes,serInfo.getSpecificSchemaSerializers().get(schemaId));
			} else {
				return 0;
			}
		} else {
			int[] indexes = serInfo.getCommonSchemaIndexTranslation(0);
			Criteria c = tupleMRConf.getCommonCriteria();
			return compare(serInfo.getCommonSchema(), c, w1, indexes, w2, indexes,serInfo.getCommonSchemaSerializers());
		}

	}


	public int compare(Schema schema, Criteria c, ITuple w1, int[] index1, ITuple w2,
	    int[] index2,Serializer[] serializers) {
		for(int i = 0; i < c.getElements().size(); i++) {
			Field field = schema.getField(i);
			SortElement e = c.getElements().get(i);
			Object o1 = w1.get(index1[i]);
			Object o2 = w2.get(index2[i]);
			Serializer serializer = (serializers == null) ? null : serializers[i];
			int comparison = compareObjects(o1, o2, e.getCustomComparator(), field.getType(),serializer);
			if(comparison != 0) {
				return(e.getOrder() == Order.ASC ? comparison : -comparison);
			}
		}
		return 0;
	}

	/**
	 * Compares two objects. Uses the given custom comparator if present. If the
	 * type is {@link Type#OBJECT} and no raw comparator is present, then a serializer
	 * comparator is used.
	 */
	@SuppressWarnings({ "unchecked" })
	public int compareObjects(Object elem1, Object elem2, RawComparator comparator,
	    Type type,Serializer serializer) {
		// If custom, just use custom.
		if(comparator != null) {
			return comparator.compare(elem1, elem2);
		}

		if(type == Type.OBJECT) {
			return serializerComparator.compare(elem1,serializer, elem2,serializer);
		} else {
			return compareObjects(elem1,elem2);
		}
	}
	
	public static int compareObjects(Object element1,Object element2){
		if(element1 == null) {
			return (element2 == null) ? 0 : -1;
		} else if(element2 == null) {
			return 1;
		} else {
			if(element1 instanceof String) {
				element1 = new Utf8((String) element1);
			}
			if(element2 instanceof String) {
				element2 = new Utf8((String) element2);
			}
			if(element1 instanceof byte[]){
				byte[] buffer1 = (byte[])element1;
				if (element2 instanceof byte[]){
					byte[] buffer2 = (byte[])element2;
					return compareBytes(buffer1,0,buffer1.length,buffer2,0,buffer2.length);
				} else if (element2 instanceof ByteBuffer){
					ByteBuffer buffer2 = (ByteBuffer)element2;
					int start2 = buffer2.arrayOffset() + buffer2.position();
					int len2 = buffer2.limit() - buffer2.position();
					return compareBytes(buffer1,0,buffer1.length,buffer2.array(),start2,len2);
				} else {
					throw new PangoolRuntimeException("Can't compare byte[] with " + element2.getClass());
				}
			} else if(element1 instanceof ByteBuffer){
				ByteBuffer buffer1 = (ByteBuffer)element1;
				int pos1 = buffer1.position();
				int start1 = buffer1.arrayOffset() + pos1;
		    int len1 = buffer1.limit() - pos1;
				if (element2 instanceof byte[]){
					byte[] buffer2 =(byte[]) element2;
					return compareBytes(buffer1.array(),start1,len1,buffer2,0,buffer2.length);
				} else if (element2 instanceof ByteBuffer){
					ByteBuffer buffer2 = (ByteBuffer)element2;
					int pos2 = buffer2.position();
					int start2 = buffer2.arrayOffset() + pos2;
			    int len2 = buffer2.limit() - pos2;
					return compareBytes(buffer1.array(),start1,len1,buffer2.array(),start2,len2);
				} else {
					throw new PangoolRuntimeException("Can't compare byte[] with " + element2.getClass());
				}
			}	else if(element1 instanceof Comparable) {
					return ((Comparable) element1).compareTo(element2);
			} else if(element2 instanceof Comparable) {
					return -((Comparable) element2).compareTo(element1);
			} else {
				throw new PangoolRuntimeException("Not comparable elements:" + element1.getClass() + " with object " + element2.getClass());
			} 
		}
	}
	

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			return (isMultipleSources) ? compareMultipleSources(b1, s1, l1, b2, s2, l2)
			    : compareOneSource(b1, s1, l1, b2, s2, l2);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected int compareMultipleSources(byte[] b1, int s1, int l1, byte[] b2, int s2,
	    int l2) throws IOException {
		Schema commonSchema = serInfo.getCommonSchema();
		Criteria commonOrder = tupleMRConf.getCommonCriteria();

		int comparison = compare(b1, s1, b2, s2, commonSchema, commonOrder, offsets);
		if(comparison != 0) {
			return comparison;
		}

		int schemaId1 = readVInt(b1, offsets.offset1);
		int schemaId2 = readVInt(b2, offsets.offset2);
		if(schemaId1 != schemaId2) {
			int r = schemaId1 - schemaId2;
			return (tupleMRConf.getSchemasOrder() == Order.ASC) ? r : -r;
		}

		int vintSize = WritableUtils.decodeVIntSize(b1[offsets.offset1]);
		offsets.offset1 += vintSize;
		offsets.offset2 += vintSize;

		// sources are the same
		Criteria criteria = tupleMRConf.getSpecificOrderBys().get(schemaId1);
		if(criteria == null) {
			return 0;
		}

		Schema specificSchema = serInfo.getSpecificSchema(schemaId1);
		return compare(b1, offsets.offset1, b2, offsets.offset2, specificSchema, criteria,
		    offsets);

	}

	private int compareOneSource(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
	    throws IOException {
		Schema commonSchema = serInfo.getCommonSchema();
		Criteria commonOrder = tupleMRConf.getCommonCriteria();
		return compare(b1, s1, b2, s2, commonSchema, commonOrder, offsets);
	}

	protected int compare(byte[] b1, int s1, byte[] b2, int s2, Schema schema,
	    Criteria criteria, Offsets o) throws IOException {
		o.offset1 = s1;
		o.offset2 = s2;
		for(int depth = 0; depth < criteria.getElements().size(); depth++) {
			Field field = schema.getField(depth);
			Field.Type type = field.getType();
			SortElement sortElement = criteria.getElements().get(depth);
			Order sort = sortElement.getOrder();
			RawComparator comparator = sortElement.getCustomComparator();

			if(comparator != null) {
				//custom comparator for OBJECT
				int length1 = WritableComparator.readVInt(b1, o.offset1);
				int length2 = WritableComparator.readVInt(b2, o.offset2);
				o.offset1 += WritableUtils.decodeVIntSize(b1[o.offset1]);
				o.offset2 += WritableUtils.decodeVIntSize(b2[o.offset2]);
				int comparison = comparator.compare(b1, o.offset1, length1, b2,
				    o.offset2, length2);
				o.offset1 += length1;
				o.offset2 += length2;
				if(comparison != 0) {
					return (sort == Order.ASC) ? comparison : -comparison;
				}
			} else {
				//not custom comparator
				switch(type){
				case INT:
				case ENUM:{
					int value1 = readVInt(b1, o.offset1);
					int value2 = readVInt(b2, o.offset2);
					if(value1 > value2) {
						return (sort == Order.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == Order.ASC) ? -1 : 1;
					}
					int vintSize = WritableUtils.decodeVIntSize(b1[o.offset1]);
					o.offset1 += vintSize;
					o.offset2 += vintSize;
				}	break;
				case LONG:{
					long value1 = readVLong(b1, o.offset1);
					long value2 = readVLong(b2, o.offset2);
					if(value1 > value2) {
						return (sort == Order.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == Order.ASC) ? -1 : 1;
					}
					int vIntSize = WritableUtils.decodeVIntSize(b1[o.offset1]);
					o.offset1 += vIntSize;
					o.offset2 += vIntSize;
				} break;
				case FLOAT:{
					float value1 = readFloat(b1, o.offset1);
					float value2 = readFloat(b2, o.offset2);
					if(value1 > value2) {
						return (sort == Order.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == Order.ASC) ? -1 : 1;
					}
					o.offset1 += Float.SIZE / 8;
					o.offset2 += Float.SIZE / 8;
				} break;
				case DOUBLE:{
					double value1 = readDouble(b1, o.offset1);
					double value2 = readDouble(b2, o.offset2);
					if(value1 > value2) {
						return (sort == Order.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == Order.ASC) ? -1 : 1;
					}
					o.offset1 += Double.SIZE / 8;
					o.offset2 += Double.SIZE / 8;
				} break;
				case BOOLEAN:{
					byte value1 = b1[o.offset1++];
					byte value2 = b2[o.offset2++];
					if(value1 > value2) {
						return (sort == Order.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == Order.ASC) ? -1 : 1;
					}
					} break;
				case STRING:
				case OBJECT:
				case BYTES:{
					int length1 = readVInt(b1, o.offset1);
					int length2 = readVInt(b2, o.offset2);
					o.offset1 += WritableUtils.decodeVIntSize(b1[o.offset1]);
					o.offset2 += WritableUtils.decodeVIntSize(b2[o.offset2]);
					int comparison = compareBytes(b1, o.offset1, length1, b2, o.offset2, length2);
					o.offset1 += length1;
					o.offset2 += length2;
					if(comparison != 0) {
						return (sort == Order.ASC) ? comparison : (-comparison);
					}
				} break;
				default:
					throw new IOException("Not supported comparison for type:" + type);
				}
			}
		}
		return 0; // equals
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
				setTupleMRConf(TupleMRConfig.get(conf));
				TupleMRConfigBuilder.initializeComparators(conf, this.tupleMRConf);
				serializerComparator.setConf(conf);
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void setTupleMRConf(TupleMRConfig config) {
		if(this.tupleMRConf != null) {
			throw new RuntimeException("TupleMR config is already set");
		}
		this.tupleMRConf = config;
		this.serInfo = tupleMRConf.getSerializationInfo();
		this.isMultipleSources = tupleMRConf.getNumIntermediateSchemas() >= 2;
	}

}
