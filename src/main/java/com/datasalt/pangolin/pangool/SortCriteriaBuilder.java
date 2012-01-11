package com.datasalt.pangolin.pangool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.RawComparator;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.pangool.SortCriteria;
import com.datasalt.pangolin.pangool.SortCriteria.SortElement;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;

/**
 * Builds an individual {@link SortCriteria} inmutable instance.
 * Links to a parent {@link SortingBuilder}.
 * 
 * @author pere
 * 
 */
@SuppressWarnings("rawtypes")
class SortCriteriaBuilder {

	protected List<SortElement> fields = new ArrayList<SortElement>();
	SortingBuilder parentSorting;
	
	SortCriteriaBuilder(SortingBuilder parentSorting) {
		this.parentSorting = parentSorting;
	}
	
	public SortCriteriaBuilder secondarySort(String sourceId) {
		return parentSorting.secondarySort(sourceId);
	}
	
	public void add(String fieldName, SortOrder order, Class<? extends RawComparator> customComparator)
	    throws InvalidFieldException {

		if(fieldAlreadyExists(fieldName)) {
			throw new InvalidFieldException("Sorting field '" + fieldName + "' already specified");
		}
		
		if(fieldName.equals(Schema.Field.SOURCE_ID_FIELD)) {
			throw new InvalidFieldException("Can't define a sorting field with reserved name: " + Schema.Field.SOURCE_ID_FIELD + ". Use appropriate API method instead.");
		}

		fields.add(new SortElement(fieldName, order, customComparator));
	}

	public SortCriteriaBuilder add(String fieldName, SortOrder order) throws InvalidFieldException {
		add(fieldName, order, null);
		return this;
	}

	private boolean fieldAlreadyExists(String fieldName) {
		for(SortElement field : fields) {
			if(field.getFieldName().equalsIgnoreCase(fieldName)) {
				return true;
			}
		}
		return false;
	}

	SortCriteria buildSortCriteria() {
		SortElement[] fieldsArray = new SortElement[fields.size()];
		fields.toArray(fieldsArray);
		return new SortCriteria(fieldsArray);
	}
	
	public Sorting buildSorting() {
		return parentSorting.buildSorting();
	}
}