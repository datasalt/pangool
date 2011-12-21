package com.datasalt.pangolin.grouper.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.io.BaseTuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.mapred.GrouperWithRollupReducer;

/**
 * 
 * This implementation of {@link ITuple} allows to mantain two {@link BaseTuple} instances in a double-buffered fashion.
 * That is, when the {@link #readFields(DataInput)} method is called , the last previous state is kept and then the new current 
 * instance is updated.
 * 
 *  
 * Since this double buffered mechanism avoids cloning or deep copying instances ,allows efficient comparison between
 * tuples performed in {@link GrouperWithRollupReducer}.
 * 
 * @author eric
 * 
 */
public class Tuple implements ITuple {

	private FieldsDescription schema;
	private Configuration conf;
	private BaseTuple previousTuple, currentTuple;

	@SuppressWarnings("unused")
	private Tuple() {
		// this allows to use private construction
		previousTuple = ReflectionUtils.newInstance(BaseTuple.class, null);
		currentTuple = ReflectionUtils.newInstance(BaseTuple.class, null);
	}

	Tuple(@Nonnull FieldsDescription schema) {
		this.schema = schema;
		currentTuple = new BaseTuple(schema);
		previousTuple = new BaseTuple(schema);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		currentTuple.write(out);

	}

	/**
	 * This is where the double-buffer swapping is performed.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// swapping double buffering
		BaseTuple tmpTuple = previousTuple;
		previousTuple = currentTuple;
		currentTuple = tmpTuple;

		currentTuple.readFields(in);
	}

	/**
	 * It always return a nonnull instance even if readFields never was invoked
	 *
	 */
	public @Nonnull BaseTuple getPreviousTuple() {
		return previousTuple;
	}

	@Override
	public int compareTo(ITuple anotherTuple) {
		return currentTuple.compareTo(anotherTuple);
	}

	@Override
	public void setConf(Configuration conf) {
		if(conf != null) {
			this.conf = conf;
			try {
				FieldsDescription schema = FieldsDescription.parse(this.conf);
				if(schema != null) {
					this.schema = schema;
				}
				previousTuple.setConf(conf);
				currentTuple.setConf(conf);
			} catch(GrouperException e) {
				throw new RuntimeException(e);
			}
		}

	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public FieldsDescription getSchema() {
		return schema;
	}

	@Override
	public int partialHashCode(String[] fields) throws InvalidFieldException {
		return currentTuple.partialHashCode(fields);
	}

	@Override
	public int getInt(String fieldName) throws InvalidFieldException {
		return currentTuple.getInt(fieldName);
	}

	@Override
	public long getLong(String fieldName) throws InvalidFieldException {
		return currentTuple.getLong(fieldName);
	}

	@Override
	public float getFloat(String fieldName) throws InvalidFieldException {
		return currentTuple.getFloat(fieldName);
	}

	@Override
	public double getDouble(String fieldName) throws InvalidFieldException {
		return currentTuple.getDouble(fieldName);
	}

	@Override
	public String getString(String fieldName) throws InvalidFieldException {
		return currentTuple.getString(fieldName);
	}

	@Override
	public Object getObject(String fieldName) throws InvalidFieldException {
		return currentTuple.getObject(fieldName);
	}

	@Override
	public Enum<? extends Enum<?>> getEnum(String fieldName) throws InvalidFieldException {
		return getEnum(fieldName);
	}

	@Override
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value) throws InvalidFieldException {
		currentTuple.setEnum(fieldName, value);

	}

	@Override
	public void setInt(String fieldName, int value) throws InvalidFieldException {
		currentTuple.setInt(fieldName, value);

	}

	@Override
	public void setString(String fieldName, String value) throws InvalidFieldException {
		currentTuple.setString(fieldName, value);

	}

	@Override
	public void setLong(String fieldName, long value) throws InvalidFieldException {
		currentTuple.setLong(fieldName, value);

	}

	@Override
	public void setFloat(String fieldName, float value) throws InvalidFieldException {
		currentTuple.setFloat(fieldName, value);

	}

	@Override
	public void setDouble(String fieldName, double value) throws InvalidFieldException {
		currentTuple.setDouble(fieldName, value);

	}

	@Override
	public void setBoolean(String fieldName, boolean value) throws InvalidFieldException {
		currentTuple.setBoolean(fieldName, value);

	}

	@Override
	public void setObject(String fieldName, Object object) throws InvalidFieldException {
		currentTuple.setObject(fieldName, object);

	}

	@Override
	public void setThriftObject(String fieldName,TBase<?,?> value) throws InvalidFieldException {
		currentTuple.setThriftObject(fieldName, value);
	}

	@Override
	public String toString() {
		return currentTuple.toString();
	}

	@Override
	public void setSchema(FieldsDescription schema) {
		currentTuple.setSchema(schema);
	}

	@Override
  public String toString(int minFieldIndex, int maxFieldIndex) throws InvalidFieldException {
	  return currentTuple.toString(minFieldIndex,maxFieldIndex);
  }

}
