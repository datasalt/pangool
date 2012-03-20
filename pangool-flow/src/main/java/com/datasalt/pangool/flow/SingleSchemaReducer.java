package com.datasalt.pangool.flow;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleReducer;

@SuppressWarnings("serial")
public abstract class SingleSchemaReducer<T, K> extends TupleReducer<T, K> {

	Tuple tuple;
	
	public SingleSchemaReducer(Schema schema) {
		this.tuple = new Tuple(schema);
	}
}
