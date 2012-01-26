//package com.datasalt.avrool;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.hadoop.io.RawComparator;
//
//import com.datasalt.avrool.SortCriteria;
//import com.datasalt.avrool.SortCriteria.SortElement;
//import com.datasalt.avrool.SortCriteria.SortOrder;
//import com.datasalt.avrool.io.tuple.ITuple.InvalidFieldException;
//
///**
// * Builds an individual {@link SortCriteria} inmutable instance.
// * Links to a parent {@link SortingBuilder}.
// * 
// * 
// * 
// */
//@SuppressWarnings("rawtypes")
//public class SortCriteriaBuilder {
//
//	protected List<SortElement> fields = new ArrayList<SortElement>();
//	SortingBuilder parentSorting;
//	
//	SortCriteriaBuilder(SortingBuilder parentSorting) {
//		this.parentSorting = parentSorting;
//	}
//	
//	public SortCriteriaBuilder secondarySort(Integer sourceId) {
//		return parentSorting.secondarySort(sourceId);
//	}
//	
//	public SortCriteriaBuilder add(String fieldName, SortOrder order, Class<? extends RawComparator> customComparator)
//	    throws InvalidFieldException {
//
//		if(fieldAlreadyExists(fieldName)) {
//			throw new InvalidFieldException("Sorting field '" + fieldName + "' already specified");
//		}
//		
//		fields.add(new SortElement(fieldName, order, customComparator));
//		return this;
//	}
//
//	public SortCriteriaBuilder add(String fieldName, SortOrder order) throws InvalidFieldException {
//		add(fieldName, order, null);
//		return this;
//	}
//
//	public SortCriteriaBuilder add(String fieldName) throws InvalidFieldException {
//		add(fieldName, SortOrder.ASC);
//		return this;
//	}
//	
//	private boolean fieldAlreadyExists(String fieldName) {
//		for(SortElement field : fields) {
//			if(field.getFieldName().equalsIgnoreCase(fieldName)) {
//				return true;
//			}
//		}
//		return false;
//	}
//
//	SortCriteria buildSortCriteria() {
//		SortElement[] fieldsArray = new SortElement[fields.size()];
//		fields.toArray(fieldsArray);
//		return new SortCriteria(fieldsArray);
//	}
//	
//	public Sorting buildSorting() throws CoGrouperException {
//		return parentSorting.buildSorting();
//	}
//}