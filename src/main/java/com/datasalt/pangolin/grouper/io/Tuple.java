package com.datasalt.pangolin.grouper.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.io.TupleImpl.InvalidFieldException;

public interface Tuple extends WritableComparable<Tuple>,Configurable{
	
	public FieldsDescription getSchema();
	
	public int partialHashCode(String[] fields) throws InvalidFieldException;
	
	
	//GETTERS
	
	public int getInt(String fieldName) throws InvalidFieldException;
	
	public long getLong(String fieldName) throws InvalidFieldException;
	
	public float getFloat(String fieldName) throws InvalidFieldException;
	
	public double getDouble(String fieldName) throws InvalidFieldException;
	
	public String getString(String fieldName) throws InvalidFieldException;
	
	public Object getObject(String fieldName) throws InvalidFieldException;
	
	public Enum<? extends Enum<?>> getEnum(String fieldName) throws InvalidFieldException;
	
	
	
	//SETTERS
	
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value) throws InvalidFieldException;
	
	public void setInt(String fieldName, int value) throws InvalidFieldException;
	
	public void setString(String fieldName,String value) throws InvalidFieldException;
	
	public void setLong(String fieldName,long value) throws InvalidFieldException;
	
	public void setFloat(String fieldName,float value) throws InvalidFieldException;
	
	public void setDouble(String fieldName,double value) throws InvalidFieldException;
	
	public void setBoolean(String fieldName,boolean value) throws InvalidFieldException;
	
	public void setObject(String fieldName,Object object) throws InvalidFieldException;
	
	public void setThriftObject(String fieldName,TBase value) throws InvalidFieldException;
	

}
