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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.tuplemr.Criteria;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
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

	protected final BinaryComparator binaryComparator = new BinaryComparator();

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
			int comparison = compare(w1.getSchema(), c, w1, indexes1, w2, indexes2);
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
				return compare(w1.getSchema(), c, w1, indexes, w2, indexes);
			} else {
				return 0;
			}
		} else {

			int[] indexes = serInfo.getCommonSchemaIndexTranslation(0);
			Criteria c = tupleMRConf.getCommonCriteria();
			return compare(w1.getSchema(), c, w1, indexes, w2, indexes);
		}

	}

	// TODO doc
	/**
	 * 
	 */
	public int compare(Schema schema, Criteria c, ITuple w1, int[] index1, ITuple w2,
	    int[] index2) {
		for(int i = 0; i < c.getElements().size(); i++) {
			Field field = schema.getField(i);
			SortElement e = c.getElements().get(i);
			Object o1 = w1.get(index1[i]);
			Object o2 = w2.get(index2[i]);
			int comparison = compareObjects(o1, o2, e.getCustomComparator(), field.getType());
			if(comparison != 0) {
				return(e.getOrder() == Order.ASC ? comparison : -comparison);
			}
		}
		return 0;
	}

	/**
	 * Compares two objects. Uses the given custom comparator if present. If the
	 * type is {@link Type#OBJECT} and no raw comparator is present, then a binary
	 * comparator is used.
	 */
	@SuppressWarnings({ "unchecked" })
	public int compareObjects(Object elem1, Object elem2, RawComparator comparator,
	    Type type) {
		// If custom, just use custom.
		if(comparator != null) {
			return comparator.compare(elem1, elem2);
		}

		if(type == Type.OBJECT) {
			return binaryComparator.compare(elem1, elem2);
		}

		Object element1 = elem1;
		Object element2 = elem2;
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
			if(element1 instanceof Comparable) {
				return ((Comparable) element1).compareTo(element2);
			} else if(element2 instanceof Comparable) {
				return -((Comparable) element2).compareTo(element1);
			} else {
				// not Comparable -> we don't care
				return 0;
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
				// Provided specific Comparator. Some field types has different
				// header length and field length.
				int[] lengths1 = getHeaderLengthAndFieldLength(b1, o.offset1, type);
				int[] lengths2 = getHeaderLengthAndFieldLength(b2, o.offset2, type);
				int dataSize1 = lengths1[1];
				int dataSize2 = lengths2[1];
				int totalField1Size = lengths1[0] + dataSize1; // Header size + data
																											 // size
				int totalField2Size = lengths2[0] + dataSize2; // Header size + data
																											 // size
				int comparison = comparator.compare(b1, o.offset1, totalField1Size, b2,
				    o.offset2, totalField2Size);
				o.offset1 += totalField1Size;
				o.offset2 += totalField2Size;
				if(comparison != 0) {
					return (sort == Order.ASC) ? comparison : -comparison;
				}
			} else if(type == Type.INT || type == Type.ENUM) {
				// VInt || Enum
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
			} else if(type == Type.LONG) {
				// VLong
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
			} else if(type == Type.FLOAT) {
				// Float
				float value1 = readFloat(b1, o.offset1);
				float value2 = readFloat(b2, o.offset2);
				if(value1 > value2) {
					return (sort == Order.ASC) ? 1 : -1;
				} else if(value1 < value2) {
					return (sort == Order.ASC) ? -1 : 1;
				}
				o.offset1 += Float.SIZE / 8;
				o.offset2 += Float.SIZE / 8;
			} else if(type == Type.DOUBLE) {
				// Double
				double value1 = readDouble(b1, o.offset1);
				double value2 = readDouble(b2, o.offset2);
				if(value1 > value2) {
					return (sort == Order.ASC) ? 1 : -1;
				} else if(value1 < value2) {
					return (sort == Order.ASC) ? -1 : 1;
				}
				o.offset1 += Double.SIZE / 8;
				o.offset2 += Double.SIZE / 8;
			} else if(type == Type.BOOLEAN) {
				// Boolean
				byte value1 = b1[o.offset1++];
				byte value2 = b2[o.offset2++];
				if(value1 > value2) {
					return (sort == Order.ASC) ? 1 : -1;
				} else if(value1 < value2) {
					return (sort == Order.ASC) ? -1 : 1;
				}
			} else if(type == Type.STRING || type == Type.OBJECT) {
				// Utf8 and Type.OBJECT compareBytes
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
			} else {
				throw new IOException("Not supported comparison for type:" + type);
			}
		}
		return 0; // equals
	}

	/**
	 * Return the header length and the field length for a field of the given type
	 * in the given position at the buffer.
	 */
	public static int[] getHeaderLengthAndFieldLength(byte[] b1, int offset1,
	    Field.Type type) throws IOException {
		switch(type) {
		case STRING:
			return new int[] { 0,
			    readVInt(b1, offset1) + WritableUtils.decodeVIntSize(b1[offset1]) };
		case INT:
		case ENUM:
			return new int[] { 0, WritableUtils.decodeVIntSize(b1[offset1]) };
		case LONG:
			return new int[] { 0, WritableUtils.decodeVIntSize(b1[offset1]) };
		case FLOAT:
			return new int[] { 0, Float.SIZE / 8 };
		case DOUBLE:
			return new int[] { 0, Double.SIZE / 8 };
		case BOOLEAN:
			return new int[] { 0, 1 };
		case OBJECT:
			return new int[] { WritableUtils.decodeVIntSize(b1[offset1]), readVInt(b1, offset1) };
		default:
			throw new IOException("Not supported type:" + type);
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
				setTupleMRConf(TupleMRConfig.get(conf));
				TupleMRConfigBuilder.initializeComparators(conf, this.tupleMRConf);
				binaryComparator.setConf(conf);
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
