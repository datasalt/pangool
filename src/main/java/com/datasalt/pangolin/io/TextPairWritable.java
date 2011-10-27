package com.datasalt.pangolin.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Partitioner;

@SuppressWarnings({ "rawtypes" })
public class TextPairWritable implements WritableComparable<TextPairWritable> {

	private Text first;
	private Text second;

	public TextPairWritable() {
		first = new Text();
		second = new Text();
	}

	public TextPairWritable(String first, String second) {
		this();
		this.first.set(first);
		this.second.set(second);
	}

	public void readFields(DataInput input) throws IOException {
		first.readFields(input);
		second.readFields(input);

	}

	public void write(DataOutput output) throws IOException {
		first.write(output);
		second.write(output);
	}

	public int compareTo(TextPairWritable that) {

		int compareTo = this.first.compareTo(((TextPairWritable) that).getFirst());
		int firstComparison = compareTo;

		return (firstComparison != 0) ? firstComparison : this.getSecond()
				.compareTo(((TextPairWritable) that).getSecond());
	}

	public static class Comparator extends WritableComparator {

		public Comparator() {
			super(TextPairWritable.class);

		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int comparison;
			int length1, length2;
			int offset1,offset2;
			try {
				length1 = readVInt(b1, s1);
				length2 = readVInt(b2, s2);
				offset1 = s1 + WritableUtils.decodeVIntSize(b1[s1]);
				offset2 = s2 + WritableUtils.decodeVIntSize(b2[s2]);
				comparison = compareBytes(b1,offset1,
						length1, b2, offset2, length2);
				
				if (comparison != 0) {
					return comparison;
				} else {
					offset1 = offset1 + length1;
					offset2 = offset2 + length2;
					
					length1 = readVInt(b1,offset1);
					length2 = readVInt(b2,offset2);
					comparison = compareBytes(b1,
							offset1 + WritableUtils.decodeVIntSize(b1[offset1]), length1, b2, offset2 + WritableUtils.decodeVIntSize(b2[offset2]), length2);
					return comparison;
				}

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}
	}

	public static class FirstStringComparator extends WritableComparator {

		public FirstStringComparator() {
			super(TextPairWritable.class);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			return ((TextPairWritable) a).first.compareTo(((TextPairWritable) b)
					.getFirst());
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int comparison, textOffset1, textOffset2;
				int length1 = readVInt(b1, s1); // text length coded in binary
				int length2 = readVInt(b2, s2);
				textOffset1 = WritableUtils.decodeVIntSize(b1[s1]);
				textOffset2 = WritableUtils.decodeVIntSize(b2[s2]);
				comparison = compareBytes(b1, s1 + textOffset1, length1, b2, s2
						+ textOffset2, length2);
				return comparison;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static class FirstStringPartitioner extends
			Partitioner<TextPairWritable, Object> {

		public int getPartition(TextPairWritable key, Object value,
				int numReduceTasks) {
			return (key.getFirst().toString().hashCode() & Integer.MAX_VALUE)
					% numReduceTasks;
		}

	}

	static { // register this comparator
		WritableComparator.define(TextPairWritable.class, new Comparator());
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{").append(first).append(",").append(second).append("}");
		return builder.toString();
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first.set(first);
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second.set(second);
	}

}
