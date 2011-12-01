package com.datasalt.pangolin.grouper.io;

import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.FieldsDescription;

public class TupleFactory {

	private FieldsDescription schema;
	
	public TupleFactory (FieldsDescription schema){
		this.schema = schema;
	}

	public Tuple createTuple(){
		Tuple tuple = ReflectionUtils.newInstance(Tuple.class, null);
		tuple.setSchema(schema);
		return tuple;
	}
	
	
}

