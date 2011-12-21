package com.datasalt.pangolin.grouper.io;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.FieldsDescription;

public class TupleFactory {

	public static Tuple createTuple(@Nonnull FieldsDescription schema){
		Tuple tuple = ReflectionUtils.newInstance(Tuple.class, null);
		tuple.setSchema(schema);
		return tuple;
	}
	
//	public static Tuple createTuple(Configuration conf){
//		try{
//		FieldsDescription schema = FieldsDescription.parse(conf);
//		} catch(GrouperException e)
//		
//		Tuple tuple = ReflectionUtils.newInstance(DoubleBufferedTuple.class, conf);
//		tuple.setSchema(schema);
//		return tuple;
//	}
	
	
}

