package com.datasalt.pangolin.grouper.io;

import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.FieldsDescription;

public class TupleFactory {

	private FieldsDescription schema;
	
	public TupleFactory (FieldsDescription schema){
		this.schema = schema;
	}

	public TupleImpl createTuple(){
		TupleImpl tuple = ReflectionUtils.newInstance(TupleImpl.class, null);
		tuple.setSchema(schema);
		return tuple;
	}
	
	
}

