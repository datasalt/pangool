package com.datasalt.pangool;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Sorting;
import com.datasalt.pangool.SortingBuilder;
import com.datasalt.pangool.SortCriteria.SortOrder;
import com.datasalt.pangool.mapreduce.GroupHandler;
import com.datasalt.pangool.mapreduce.GroupHandlerWithRollup;
import com.datasalt.pangool.mapreduce.InputProcessor;

@SuppressWarnings("rawtypes")
public class BaseCoGrouperTest {

	protected GroupHandler myGroupHandler = new GroupHandler();
	
	protected GroupHandler myGroupHandlerWithRollup = new GroupHandlerWithRollup();

	protected InputProcessor myInputProcessor = new InputProcessor();
	
	protected Sorting getTestSorting() throws InvalidFieldException, CoGrouperException {
		return new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.DESC)
			.addSourceId(SortOrder.ASC)
			.secondarySort(1).add("content", SortOrder.ASC)
			.secondarySort(2).add("name", SortOrder.ASC)
			.buildSorting();
	}
}
