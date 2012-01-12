package com.datasalt.pangolin.pangool;

import java.util.HashMap;
import java.util.Map;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;

/**
 * Builds an inmutable {@link Sorting} instance.
 * May have childs {@link SortCriteriaBuilder}.
 * 
 * @author pere
 * 
 */
public class SortingBuilder extends SortCriteriaBuilder {

	private Map<String, SortCriteriaBuilder> secondarySortBuilders;

	public SortingBuilder() {
		super(null);
		secondarySortBuilders = new HashMap<String, SortCriteriaBuilder>();
	}
	
	public SortCriteriaBuilder secondarySort(String sourceId) {
		SortCriteriaBuilder builder = new SortCriteriaBuilder(this);
		secondarySortBuilders.put(sourceId, builder);
		return builder;
	}

	public void addSourceId(SortOrder order) throws InvalidFieldException {
		add(Schema.Field.SOURCE_ID_FIELD, order, null);
	}
	
	public Sorting buildSorting() {
		Map<String, SortCriteria> secondarySortCriterias = new HashMap<String, SortCriteria>();
		for(Map.Entry<String, SortCriteriaBuilder> builders: secondarySortBuilders.entrySet()) {
			secondarySortCriterias.put(builders.getKey(), builders.getValue().buildSortCriteria());
		}
		Sorting sorting = new Sorting(buildSortCriteria(), secondarySortCriterias);
		return sorting;
	}
}
