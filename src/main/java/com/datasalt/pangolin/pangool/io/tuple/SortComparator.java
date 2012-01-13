package com.datasalt.pangolin.pangool.io.tuple;

import static org.apache.hadoop.io.WritableComparator.compareBytes;
import static org.apache.hadoop.io.WritableComparator.readDouble;
import static org.apache.hadoop.io.WritableComparator.readFloat;
import static org.apache.hadoop.io.WritableComparator.readInt;
import static org.apache.hadoop.io.WritableComparator.readLong;
import static org.apache.hadoop.io.WritableComparator.readVInt;
import static org.apache.hadoop.io.WritableComparator.readVLong;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.pangool.CoGrouperException;
import com.datasalt.pangolin.pangool.PangoolConfig;
import com.datasalt.pangolin.pangool.PangoolConfigBuilder;
import com.datasalt.pangolin.pangool.Schema;
import com.datasalt.pangolin.pangool.Schema.Field;
import com.datasalt.pangolin.pangool.SortCriteria;
import com.datasalt.pangolin.pangool.SortCriteria.SortElement;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;

/**
 * 
 * @author pere
 *
 */
@SuppressWarnings("rawtypes")
public class SortComparator implements RawComparator<ITuple>, Configurable {

	private Configuration conf;
	private PangoolConfig config;

	private Map<Class, RawComparator> instancedComparators;

	protected SortCriteria commonCriteria;
	protected Schema commonSchema;
	
	PangoolConfig getConfig() {
  	return config;
  }

	/*
	 * When comparing, we save the source Ids, if we find them
	 */
	int firstSourceId  = 0;
	int secondSourceId = 0;

	int offset1 = 0;
	int offset2 = 0;
	
	int nSchemas = 0; // Cached number of schemas
	
	public SortComparator() {

	}

	/**
	 * Called for each compare(), we must reset the source ids for both tuples
	 */
	private void resetSourceIds() {
		firstSourceId  = 0;
		secondSourceId = 0;
	}
	
	/**
	 * Never called in MapRed jobs. Just for completion and test purposes
	 */
	@Override
	public int compare(ITuple w1, ITuple w2) {
		resetSourceIds();
		
		int fieldsToCompare = commonCriteria.getSortElements().length;
		int commonCompare = compare(fieldsToCompare, commonSchema, commonCriteria, w1, w2);
		
		if(commonCompare == 0) {
			if(nSchemas == 1) {
				// If we have only one schema, everything is common
				return 0;
			} else {
				// Continue comparing
				if(firstSourceId == secondSourceId) {
					SortCriteria particularCriteria = config.getSorting().getSpecificCriteriaByName(firstSourceId);
					if(particularCriteria != null) {
						Schema particularSchema = config.getSpecificOrderedSchemas().get(firstSourceId);
						fieldsToCompare = particularCriteria.getSortElements().length;
						return compare(fieldsToCompare, particularSchema, particularCriteria, w1, w2);
					} else {
						// No particular ordering, we don't care
						return 0;
					}
				} else { // Different sources, order by sourceId
					return firstSourceId > secondSourceId ? 1 : -1;
				}
			}
		} else {
			return commonCompare;
		}
	}

	/**
	 * Never called in MapRed jobs. Just for completion and test purposes
	 */
	@SuppressWarnings("unchecked")
	public int compare(int fieldsToCompare, Schema schema, SortCriteria sortCriteria, ITuple w1, ITuple w2) {
		ITuple tuple1 = (ITuple) w1;
		ITuple tuple2 = (ITuple) w2;
		for(int depth = 0; depth < fieldsToCompare; depth++) {
			Field field = schema.getField(depth);
			String fieldName = field.getName();
			SortElement sortElement = sortCriteria.getSortElementByFieldName(field.getName());
			SortOrder sort = SortOrder.ASC; // by default
			RawComparator comparator = null;
			if(sortElement != null) {
				sort = sortElement.getSortOrder();
				comparator = instancedComparators.get(sortElement.getComparator());
			} else {
				throw new RuntimeException("Fatal error : Trying to sort by field '" + fieldName
				    + "' but not present in sortCriteria:" + sortCriteria);
			}
			int comparison;
			if(comparator != null) {
				comparison = comparator.compare(tuple1.getObject(fieldName), tuple2.getObject(fieldName));
			} else {
				comparison = compareObjects(tuple1.getObject(fieldName), tuple2.getObject(fieldName));
			}
			if(comparison != 0) {
				return (sort == SortOrder.ASC) ? comparison : -comparison;
			}
		}
		return 0;
	}

	/**
	 * Compares two objects
	 * 
	 */
	@SuppressWarnings({ "unchecked" })
	public static int compareObjects(Object element1, Object element2) {
		if(element1 == null) {
			return (element2 == null) ? 0 : -1;
		} else if(element2 == null) {
			return 1;
		} else {
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
		resetSourceIds();

		SortCriteria commonCriteria = config.getSorting().getSortCriteria();
		Schema commonSchema = config.getCommonOrderedSchema();
		int fieldsToCompare = commonCriteria.getSortElements().length;		

		int commonCompare = compare(fieldsToCompare, commonSchema, commonCriteria, b1, s1, l1, b2, s2, l2);

		if(commonCompare == 0) {
			if(nSchemas == 1) {
				// If we have only one schema, everything is common
				return 0;
			} else {
				// Continue comparing
				if(firstSourceId == secondSourceId) {
					SortCriteria particularCriteria = config.getSorting().getSpecificCriteriaByName(firstSourceId);
					if(particularCriteria != null) {
						Schema particularSchema = config.getSpecificOrderedSchemas().get(firstSourceId);
						fieldsToCompare = particularCriteria.getSortElements().length;
						return compare(fieldsToCompare, particularSchema, particularCriteria, b1, offset1, l1, b2, offset2, l2);
					} else {
						// No particular ordering, we don't care
						return 0;
					}
				} else { // Different sources, order by sourceId
					return firstSourceId > secondSourceId ? 1 : -1;
				}
			}
		} else {
			return commonCompare;
		}
	}

	/**
	 * Cache RawComparators
	 */
  private void instanceComparators() {
		this.instancedComparators = new HashMap<Class, RawComparator>();
		for(SortElement sortElement : config.getSorting().getSortCriteria().getSortElements()) {
			Class<? extends RawComparator> clazz = sortElement.getComparator();
			if(clazz != null) {
				RawComparator comparator = ReflectionUtils.newInstance(clazz, conf);
				instancedComparators.put(clazz, comparator);
			}
		}
	}

	/**
	 * Compares {@link ITuple} objects serialized in binary up to a maximum depth specified in <b>maxFieldsCompared</b>
	 * 
	 * @param fieldsToCompare
	 * 
	 */
	public int compare(int fieldsToCompare, Schema schema, SortCriteria sortCriteria, byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		try {
			offset1 = s1;
			offset2 = s2;
			for(int depth = 0; depth < fieldsToCompare; depth++) {
				Field field = schema.getFields().get(depth);
				Class<?> type = field.getType();
				SortElement sortElement = sortCriteria.getSortElementByFieldName(field.getName());
				SortOrder sort = SortOrder.ASC; // by default
				RawComparator<?> comparator = null;
				if(sortElement != null) {
					sort = sortElement.getSortOrder();
					comparator = instancedComparators.get(sortElement.getComparator());
				}
				if(comparator != null) {
					// Provided specific Comparator
					int length1 = readVInt(b1, offset1);
					int length2 = readVInt(b2, offset2);
					offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
					offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
					int comparison = comparator.compare(b1, offset1, length1, b2, offset2, length2);
					if(comparison != 0) {
						return (sort == SortOrder.ASC) ? comparison : -comparison;
					}
					offset1 += length1;
					offset2 += length2;
				} else if(type == Integer.class) {
					// Integer
					int value1 = readInt(b1, offset1);
					int value2 = readInt(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASC) ? -1 : 1;
					}
					offset1 += Integer.SIZE / 8;
					offset2 += Integer.SIZE / 8;
				} else if(type == Long.class) {
					// Long
					long value1 = readLong(b1, offset1);
					long value2 = readLong(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASC) ? -1 : 1;
					}
					offset1 += Long.SIZE / 8;
					offset2 += Long.SIZE / 8;
				} else if(type == VIntWritable.class || type.isEnum()) {
					// VInt || Enum
					int value1 = readVInt(b1, offset1);
					int value2 = readVInt(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASC) ? -1 : 1;
					}
					int vintSize = WritableUtils.decodeVIntSize(b1[offset1]);
					offset1 += vintSize;
					offset2 += vintSize;
				} else if(type == VLongWritable.class) {
					// VLong
					long value1 = readVLong(b1, offset1);
					long value2 = readVLong(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASC) ? -1 : 1;
					}
					int vIntSize = WritableUtils.decodeVIntSize(b1[offset1]);
					offset1 += vIntSize;
					offset2 += vIntSize;
				} else if(type == Float.class) {
					// Float
					float value1 = readFloat(b1, offset1);
					float value2 = readFloat(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASC) ? -1 : 1;
					}
					offset1 += Float.SIZE / 8;
					offset2 += Float.SIZE / 8;
				} else if(type == Double.class) {
					// Double
					double value1 = readDouble(b1, offset1);
					double value2 = readDouble(b2, offset2);
					if(value1 > value2) {
						return (sort == SortOrder.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASC) ? -1 : 1;
					}
					offset1 += Double.SIZE / 8;
					offset2 += Double.SIZE / 8;
				} else if(type == Boolean.class) {
					// Boolean
					byte value1 = b1[offset1++];
					byte value2 = b2[offset2++];
					if(value1 > value2) {
						return (sort == SortOrder.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == SortOrder.ASC) ? -1 : 1;
					}
				} else {
					// String(Text) and the rest of types using compareBytes
					int length1 = readVInt(b1, offset1);
					int length2 = readVInt(b2, offset2);
					offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
					offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
					int comparison = compareBytes(b1, offset1, length1, b2, offset2, length2);
					if(comparison != 0) {
						return (sort == SortOrder.ASC) ? comparison : (-comparison);
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
		if(conf != null) {
			this.conf = conf;
			/*
			 * Set PangoolConf
			 */
			try {
	      config = PangoolConfigBuilder.get(conf);
	      instanceComparators();
	      nSchemas = config.getSchemes().values().size();
	      commonCriteria = config.getSorting().getSortCriteria();
	      commonSchema = config.getCommonOrderedSchema();
      } catch(JsonParseException e) {
      	throw new RuntimeException(e);
      } catch(JsonMappingException e) {
      	throw new RuntimeException(e);
      } catch(IOException e) {
      	throw new RuntimeException(e);
      } catch(CoGrouperException e) {
      	throw new RuntimeException(e);
      }
		}
	}
}
