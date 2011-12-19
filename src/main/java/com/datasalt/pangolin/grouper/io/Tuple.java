package com.datasalt.pangolin.grouper.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.io.TupleImpl.InvalidFieldException;

/**
 * This is the common interface implemented by {@link TupleImpl} and {@link DoubleBufferedTuple}.
 * A Tuple is basically a hadoop-serializable object containing fields according to the schema defined in 
 * {@link FieldsDescription}. Tuples are used in intermediate {@link org.apache.hadoop.mapreduce.Mapper}
 * outputs in {@link Grouper} and {@link Grouper}
 * 
 * @author eric
 *
 */
public interface Tuple extends WritableComparable<Tuple>,Configurable{
	
	public FieldsDescription getSchema();
	
	public int partialHashCode(String[] fields) throws InvalidFieldException;
	
	public void setSchema(FieldsDescription schema);
	
	
	//Getters
	
	public int getInt(String fieldName) throws InvalidFieldException;
	
	public long getLong(String fieldName) throws InvalidFieldException;
	
	public float getFloat(String fieldName) throws InvalidFieldException;
	
	public double getDouble(String fieldName) throws InvalidFieldException;
	
	public String getString(String fieldName) throws InvalidFieldException;
	
	public Object getObject(String fieldName) throws InvalidFieldException;
	
	public Enum<? extends Enum<?>> getEnum(String fieldName) throws InvalidFieldException;
	
	
	
	//Setters
	
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value) throws InvalidFieldException;
	
	public void setInt(String fieldName, int value) throws InvalidFieldException;
	
	public void setString(String fieldName,String value) throws InvalidFieldException;
	
	public void setLong(String fieldName,long value) throws InvalidFieldException;
	
	public void setFloat(String fieldName,float value) throws InvalidFieldException;
	
	public void setDouble(String fieldName,double value) throws InvalidFieldException;
	
	public void setBoolean(String fieldName,boolean value) throws InvalidFieldException;
	
	public void setObject(String fieldName,Object object) throws InvalidFieldException;
	
	public void setThriftObject(String fieldName,TBase<?, ?> value) throws InvalidFieldException;
	

	public String toString(int minFieldIndex,int maxFieldIndex) throws InvalidFieldException;
	
}
