//package com.datasalt.pangool;
//
//import junit.framework.Assert;
//
//import org.apache.hadoop.io.RawComparator;
//import org.junit.Test;
//
//import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
//import com.datasalt.pangool.SortCriteria;
//import com.datasalt.pangool.SortCriteriaBuilder;
//import com.datasalt.pangool.SortCriteria.SortOrder;
//
///**
// * 
// * @author pere
// *
// */
//public class TestSortCriteriaBuilder {
//
//	@Test
//	public void testInvalidDuplicatedField() throws InvalidFieldException {
//		SortCriteriaBuilder builder = new SortCriteriaBuilder(null);
//		builder.add("url", SortOrder.ASC);
//		try {
//			builder.add("url", SortOrder.ASC);
//			Assert.fail("Catched exception after adding same field twice should have been thrown");
//		} catch(Exception ife) {
//			return;
//		}
//	}
//
//	public void testInvalidSortOrder() {
//		SortCriteriaBuilder builder = new SortCriteriaBuilder(null);
//		try {
//			builder.add("whatever", null);
//			Assert.fail("Catched exception after adding null sort order should have been thrown");
//		} catch(Exception ife) {
//			return;
//		}		
//	}
//	
//	@Test
//	public void testCreateSortCriteria() throws InvalidFieldException {
//		SortCriteriaBuilder builder = new SortCriteriaBuilder(null);
//		RawComparator<Object> comparator = new RawComparator<Object>() {
//
//			@Override
//      public int compare(Object arg0, Object arg1) {
//	      return 0;
//      }
//
//			@Override
//      public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
//	      return 0;
//      }
//		};
//		builder.add("oneField", SortOrder.ASC);
//		builder.add("otherField", SortOrder.DESC, comparator.getClass());
//		SortCriteria sortCriteria = builder.buildSortCriteria();
//		Assert.assertNotNull(sortCriteria);
//		Assert.assertEquals(SortOrder.ASC, sortCriteria.getSortElements()[0].getSortOrder());
//		Assert.assertEquals(SortOrder.DESC, sortCriteria.getSortElements()[1].getSortOrder());
//		Assert.assertEquals(comparator.getClass(), sortCriteria.getSortElements()[1].getComparator());
//	}
//}
