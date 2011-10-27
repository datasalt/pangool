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
public class TextLongWritable  implements WritableComparable<TextLongWritable>{
	
	private Text thisText;
	private long thisLong;
	
	public TextLongWritable() {
		thisText = new Text();
	}

	public TextLongWritable(String first, long second) {
		this();
		this.thisText.set(first);
		this.thisLong = second;
	}
	
	public void readFields(DataInput input) throws IOException {
		thisText.readFields(input);
		this.thisLong = input.readLong();
	}

	public void write(DataOutput output) throws IOException {
		thisText.write(output);
		output.writeLong(thisLong);		
	}
	
	public int compareTo(TextLongWritable that){
		
		int firstComparison = this.thisText.compareTo(((TextLongWritable)that).getText());
		if(firstComparison != 0){
			return firstComparison;
		} else {
			long thisValue = this.thisLong;
	    long thatValue = that.getLong();
	    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		}
	}
	
	
	public static class Comparator extends WritableComparator {

		public Comparator() {
			super(TextLongWritable.class);
			
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int comparison;
			int length1, length2;

			try {
				length1 = readVInt(b1, s1);
				length2 = readVInt(b2, s2);
				comparison = compareBytes(b1,
						s1 + WritableUtils.decodeVIntSize(b1[s1]), length1, b2, s2
								+ WritableUtils.decodeVIntSize(b2[s2]), length2);

				if (comparison != 0) {
					return comparison;
				} else {

					long thisValue = readLong(b1, s1 + l1 - Long.SIZE / 8);
					long thatValue = readLong(b2, s2 + l2 - Long.SIZE / 8);
					int returnV = (thisValue < thatValue ? -1
							: (thisValue == thatValue ? 0 : 1));
					return returnV;
				}

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}
	}
	
	public static class TextElementComparator extends WritableComparator {
		
	  public TextElementComparator() {
      super(TextLongWritable.class);
    }
	  
		public int compare(WritableComparable a , WritableComparable b){
			return ((TextLongWritable)a).thisText.compareTo(((TextLongWritable)b).getText());			
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int length1 = readVInt(b1, s1); // text length coded in binary
				int length2 = readVInt(b2, s2);
				int textOffset1 = WritableUtils.decodeVIntSize(b1[s1]);
				int textOffset2 = WritableUtils.decodeVIntSize(b2[s2]);
				return compareBytes(b1, s1 + textOffset1, length1, b2,
						s2 + textOffset2, length2);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public static class TextHashPartitioner extends Partitioner<TextLongWritable,Object> {

		public int getPartition(TextLongWritable key, Object value, int numReduceTasks) {
			return (key.getText().toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}

	}
	
	static {                                        // register this comparator
	    WritableComparator.define(TextLongWritable.class, new Comparator());
	}
	
	public String toString(){
		StringBuilder builder = new StringBuilder();
		builder.append("{").append(thisText).append(",").append(thisLong).append("}");
		return builder.toString();
	}
	
	
	public Text getText(){
		return thisText;
	}
	
	public void setText(String first){
		this.thisText.set(first);
	}
	
	
	public long getLong(){
		return thisLong;
	}

	public void setLong(long second){
		this.thisLong = second;
	}		

}
