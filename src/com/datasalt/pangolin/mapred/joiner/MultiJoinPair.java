package com.datasalt.pangolin.mapred.joiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This is the entity that will be serialized as Key when using the {@link MultiJoiner} Subclasses of this class may
 * establish a WritableComparable class that could be used for secondary sorting. By default, secondary sorting is only
 * applied at the level of the "channel" (see the MultiJoiner API). See {@link MultiJoinPairText} for an example of
 * subclass of this class.
 * 
 * @author pere
 * 
 * @param <K>
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class MultiJoinPair<K extends WritableComparable> implements WritableComparable<MultiJoinPair> {

	private BytesWritable group = new BytesWritable();
	private int channelId = 0;
	private static final int INTEGER_BYTES = 4;
	private K secondarySort;
	private Class<K> secondarySortClass;

	public MultiJoinPair() throws InstantiationException, IllegalAccessException {
		this(null);
	}

	public MultiJoinPair(Class<K> secondaryKeyClass) throws InstantiationException, IllegalAccessException {
		this.secondarySortClass = secondaryKeyClass;
		if(this.secondarySortClass != null) {
			this.secondarySort = secondaryKeyClass.newInstance();
		}
	}

	public int getChannelId() {
		return channelId;
	}

	public void setChannelId(int channel) {
		this.channelId = channel;
	}

	public BytesWritable getMultiJoinGroup() {
		return group;
	}

	public void setMultiJoinGroup(byte[] groupKey) {
		this.group.set(groupKey, 0, groupKey.length);
	}

	public void setMultiJoinGroup(byte[] groupKey, int offset, int length) {
		this.group.set(groupKey, offset, length);
	}

	public void setMultiJoinGroup(BytesWritable groupKey) {
		this.group.set(groupKey);
	}

	public K getSecondarySort() {
		return secondarySort;
	}

	public void setSecondSort(K second) {
		this.secondarySort = second;
	}

	public Class<K> getSecondarySortClass() {
		return secondarySortClass;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		group.write(out);
		WritableUtils.writeVInt(out, channelId);
		if(secondarySortClass != null) {
			secondarySort.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		group.readFields(in);
		channelId = WritableUtils.readVInt(in);
		if(secondarySortClass != null) {
			secondarySort.readFields(in);
		}
	}

	public boolean equals(Object o) {
		if(!(o instanceof MultiJoinPair<?>)) {
			return false;
		}
		MultiJoinPair<?> other = (MultiJoinPair<?>) o;
		return(group.equals(other.group) && (channelId == other.channelId) && (secondarySortClass == null || secondarySort
		    .equals(other.secondarySort)));
	}

	public int compareTo(MultiJoinPair other) {
		int res = group.compareTo(other.group);
		if(res == 0) {
			if(channelId == other.channelId) {
				if(secondarySortClass != null) {
					return secondarySort.compareTo(other.secondarySort);
				} else {
					return 0;
				}
			} else {
				return channelId > other.channelId ? 1 : -1;
			}
		} else {
			return res;
		}
	}

	public int hashCode() {
		if(secondarySort != null) {
			return ((group.hashCode() * 31 + channelId) * 31 + secondarySort.hashCode()) & Integer.MAX_VALUE;
		}
		return (group.hashCode() * 31 + channelId) & Integer.MAX_VALUE;
	}

	/**
	 * The binary comparator for the {@link MultiJoinPair}
	 * 
	 * @author pere
	 * 
	 */
	public static class Comparator extends WritableComparator {

		Class<? extends WritableComparable> cl;

		public Comparator(Class<? extends WritableComparable> cl) {
			super(MultiJoinPair.class, true);
			this.cl = cl;
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int size1 = readInt(b1, s1);
				int size2 = readInt(b2, s2);
				int offset1, offset2;
				int cmp = WritableComparator.compareBytes(b1, s1 + INTEGER_BYTES, size1, b2, s2 + INTEGER_BYTES, size2);
				if(cmp == 0) {
					offset1 = s1 + INTEGER_BYTES + size1;
					offset2 = s2 + INTEGER_BYTES + size2;
					int secondarySort1 = readVInt(b1, offset1);
					int secondarySort2 = readVInt(b2, offset2);
					int variableSize1 = WritableUtils.decodeVIntSize(b1[offset1]);
					int variableSize2 = WritableUtils.decodeVIntSize(b2[offset2]);
					if(secondarySort1 == secondarySort2) {
						if(cl != null) {
							WritableComparator comparator = WritableComparator.get(cl);
							offset1 = offset1 + variableSize1;
							offset2 = offset2 + variableSize2;
							return comparator.compare(b1, offset1, l1 - offset1 + s1, b2, offset2, l2 - offset2 + s2);
						} else {
							return 0;
						}
					} else {
						return secondarySort1 > secondarySort2 ? 1 : -1;
					}
				} else {
					return cmp;
				}
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * The Partitioner that partitions only by the first byte array
	 * 
	 * @author pere
	 * 
	 */
	public static class GroupPartitioner extends Partitioner {

		@Override
		public int getPartition(Object key, Object value, int numPartitions) {
			MultiJoinPair pair = (MultiJoinPair) key;
			return ((pair.group.hashCode()) & Integer.MAX_VALUE) % numPartitions;
		}
	}

	/**
	 * The Comparator that can be used as a Group Comparator by comparing only the first byte array
	 * 
	 * @author pere,eric
	 * 
	 */
	public static class GroupComparator extends WritableComparator {

		public GroupComparator() {
			super(MultiJoinPair.class, true);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int size1 = readInt(b1, s1);
			int size2 = readInt(b2, s2);
			return WritableComparator.compareBytes(b1, s1 + INTEGER_BYTES, size1, b2, s2 + INTEGER_BYTES, size2);

		}
	}

	static { // register this comparator
		WritableComparator.define(MultiJoinPair.class, new Comparator(null));
	}
}