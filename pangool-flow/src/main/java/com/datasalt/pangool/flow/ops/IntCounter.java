package com.datasalt.pangool.flow.ops;

import java.io.Serializable;

import com.datasalt.pangool.io.ITuple;

@SuppressWarnings("serial")
public class IntCounter implements Serializable {

	int count = 0;
	String field;
	
	public IntCounter(String field) {
		this.field = field;
	}
	
	public int count(Iterable<ITuple> tuples) {
		for(ITuple tuple: tuples) {
			count += (Integer) tuple.get(field);
		}
		return count;
	}
}
