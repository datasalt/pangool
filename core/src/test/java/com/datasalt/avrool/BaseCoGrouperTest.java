//package com.datasalt.avrool;
//
//import java.io.IOException;
//
//import com.datasalt.avrool.CoGrouperException;
//import com.datasalt.avrool.Sorting;
//import com.datasalt.avrool.SortingBuilder;
//import com.datasalt.avrool.SortCriteria.SortOrder;
//import com.datasalt.avrool.api.GroupHandler;
//import com.datasalt.avrool.api.GroupHandlerWithRollup;
//import com.datasalt.avrool.api.InputProcessor;
//import com.datasalt.avrool.io.tuple.ITuple.InvalidFieldException;
//
//@SuppressWarnings("rawtypes")
//public abstract class BaseCoGrouperTest {
//
//	protected GroupHandler myGroupHandler = new GroupHandler();
//
//	protected GroupHandler myGroupHandlerWithRollup = new GroupHandlerWithRollup();
//
//	protected InputProcessor myInputProcessor = new InputProcessor() {
//
//		@Override
//		public void process(Object key, Object value, CoGrouperContext context, Collector collector) throws IOException,
//		    InterruptedException {
//
//		}
//	};
//
//	protected Sorting getTestSorting() throws InvalidFieldException, CoGrouperException {
//
//		return new SortingBuilder()
//			.add("url", SortOrder.ASC)
//			.add("date", SortOrder.DESC)
//			.addSourceId(SortOrder.ASC)
//		  .secondarySort(1)
//		  	.add("content", SortOrder.ASC)
//		  .secondarySort(2)
//		   	.add("name", SortOrder.ASC)
//		  .buildSorting();
//	}
//}
