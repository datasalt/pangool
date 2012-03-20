package com.datasalt.pangool.flow.mapred;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMapper;

@SuppressWarnings("serial")
public abstract class SingleSchemaMapper<T, K> extends TupleMapper<T, K> {

	protected Tuple tuple;
	
	public SingleSchemaMapper(Schema schema) {
		this.tuple = new Tuple(schema);
	}
}
