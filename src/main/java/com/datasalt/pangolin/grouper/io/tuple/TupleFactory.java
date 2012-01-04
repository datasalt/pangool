package com.datasalt.pangolin.grouper.io.tuple;

import javax.annotation.Nonnull;

import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.grouper.Schema;

public class TupleFactory {

	public static Tuple createTuple(@Nonnull Schema schema){
		Tuple tuple = ReflectionUtils.newInstance(Tuple.class, null);
		tuple.setSchema(schema);
		return tuple;
	}
	
}

