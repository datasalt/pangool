package com.datasalt.pangool.mapreduce;

import static org.apache.hadoop.io.WritableComparator.compareBytes;
import static org.apache.hadoop.io.WritableComparator.readDouble;
import static org.apache.hadoop.io.WritableComparator.readFloat;
import static org.apache.hadoop.io.WritableComparator.readInt;
import static org.apache.hadoop.io.WritableComparator.readLong;
import static org.apache.hadoop.io.WritableComparator.readVInt;
import static org.apache.hadoop.io.WritableComparator.readVLong;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SerializationInfo;
import com.datasalt.pangool.Criteria;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Criteria.SortElement;
import com.datasalt.pangool.io.tuple.ITuple;

@SuppressWarnings("rawtypes")
public class SortComparator implements RawComparator<ITuple>, Configurable {

	protected Configuration conf;
	protected CoGrouperConfig grouperConf;
	protected SerializationInfo serInfo;
	
	private static final class Offsets {
		protected int offset1=0;
		protected int offset2=0;
	}
	protected Offsets offsets = new Offsets();
	protected boolean isMultipleSources;
	

	protected CoGrouperConfig getConfig() {
		return grouperConf;
	}

	public SortComparator() {}
	
	/**
	 * Never called in MapRed jobs. Just for completion and test purposes
	 */
	@Override
	public int compare(ITuple w1, ITuple w2) {
		//TODO 
		return 0;
	}

	/**
	 * Compares two objects
	 */
	@SuppressWarnings({ "unchecked" })
	public static int compareObjects(Object elem1, Object elem2) {
		Object element1 = elem1;
		Object element2 = elem2;
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
		try{
			return (isMultipleSources) ? compareMultipleSources(b1,s1,l1,b2,s2,l2) : compareOneSource(b1,s1,l1,b2,s2,l2);
		} catch(IOException e){
			throw new RuntimeException(e);
		}
	}
	
	private int compareMultipleSources(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2) throws IOException {
		Schema commonSchema = serInfo.getCommonSchema();
		Criteria commonOrder = grouperConf.getCommonSortBy();

		int comparison = compare(b1,s1,b2,s2,commonSchema,commonOrder,offsets);
		if (comparison != 0){
			return comparison;
		}
		
		int sourceId1 = readVInt(b1, offsets.offset1);
		int sourceId2 = readVInt(b2, offsets.offset2);
		if(sourceId1 > sourceId2) {
			return  1;  
		} else if(sourceId1 < sourceId2) {
			return -1;
		}
		int vintSize = WritableUtils.decodeVIntSize(b1[offsets.offset1]);
		offsets.offset1 += vintSize;
		offsets.offset2 += vintSize;
		
		//sources are the same
		Criteria criteria = grouperConf.getSecondarySortBys().get(sourceId1); 
		if (criteria == null){
			return 0;
		}
		
		Schema specificSchema = serInfo.getSpecificSchema(sourceId1);
		return compare(b1,offsets.offset1,b2,offsets.offset2,specificSchema,criteria,offsets);
	
	}
	
	private int compareOneSource(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2) throws IOException {
		Schema commonSchema = serInfo.getCommonSchema();
		Criteria commonOrder = grouperConf.getCommonSortBy();
		return compare(b1,s1,b2,s2,commonSchema,commonOrder,offsets);
	}
	
	protected int compare(byte[] b1, int s1,byte[] b2, int s2,Schema schema,Criteria criteria,Offsets o) throws IOException {
			o.offset1 = s1;
			o.offset2 = s2;
			for(int depth = 0; depth < criteria.getElements().size(); depth++) {
				Field field = schema.getField(depth);
				Class<?> type = field.getType();
				SortElement sortElement = criteria.getElements().get(depth);
				Order sort = sortElement.getOrder();
				Class<? extends RawComparator> comparatorClass =sortElement.getCustomComparator(); 
				RawComparator comparator = null; //TODO fix this
				
				if(comparator != null) {
					// Provided specific Comparator
					int length1 = readVInt(b1, o.offset1);
					int length2 = readVInt(b2, o.offset2);
					o.offset1 += WritableUtils.decodeVIntSize(b1[o.offset1]);
					o.offset2 += WritableUtils.decodeVIntSize(b2[o.offset2]);
					int comparison = comparator.compare(b1, o.offset1, length1, b2, o.offset2, length2);
					if(comparison != 0) {
						return (sort == Order.ASC) ? comparison : -comparison;
					}
					o.offset1 += length1;
					o.offset2 += length2;
				} else if(type == Integer.class) {
					// Integer
					int value1 = readInt(b1, o.offset1);
					int value2 = readInt(b2, o.offset2);
					if(value1 > value2) {
						return (sort == Order.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == Order.ASC) ? -1 : 1;
					}
					o.offset1 += Integer.SIZE / 8;
					o.offset2 += Integer.SIZE / 8;
				} else if(type == Long.class) {
					// Long
					long value1 = readLong(b1, o.offset1);
					long value2 = readLong(b2, o.offset2);
					if(value1 > value2) {
						return (sort == Order.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == Order.ASC) ? -1 : 1;
					}
					o.offset1 += Long.SIZE / 8;
					o.offset2 += Long.SIZE / 8;
				} else if(type == VIntWritable.class || type.isEnum()) {
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
				} else if(type == VLongWritable.class) {
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
				} else if(type == Float.class) {
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
				} else if(type == Double.class) {
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
				} else if(type == Boolean.class) {
					// Boolean
					byte value1 = b1[o.offset1++];
					byte value2 = b2[o.offset2++];
					if(value1 > value2) {
						return (sort == Order.ASC) ? 1 : -1;
					} else if(value1 < value2) {
						return (sort == Order.ASC) ? -1 : 1;
					}
				} else {
					// String(Text) and the rest of types using compareBytes
					int length1 = readVInt(b1, o.offset1);
					int length2 = readVInt(b2, o.offset2);
					o.offset1 += WritableUtils.decodeVIntSize(b1[o.offset1]);
					o.offset2 += WritableUtils.decodeVIntSize(b2[o.offset2]);
					int comparison = compareBytes(b1, o.offset1, length1, b2, o.offset2, length2);
					if(comparison != 0) {
						return (sort == Order.ASC) ? comparison : (-comparison);
					}
					o.offset1 += length1;
					o.offset2 += length2;
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
				grouperConf = CoGrouperConfig.get(conf);
				this.serInfo = grouperConf.getSerializationInfo();
				this.isMultipleSources = grouperConf.getNumSources() >= 2;
				
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}
