package com.datasalt.pangool.io.tuple;

import com.datasalt.pangolin.grouper.io.tuple.ITuple;

public interface ISourcedTuple extends ITuple {
	
	public ITuple getContainedTuple();
	public void setContainedTuple(ITuple tuple);
	
}
