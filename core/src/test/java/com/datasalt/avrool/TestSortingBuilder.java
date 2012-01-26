package com.datasalt.avrool;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Sorting;
import com.datasalt.avrool.SortingBuilder;
import com.datasalt.avrool.SortCriteria.SortOrder;
import com.datasalt.avrool.io.tuple.ITuple.InvalidFieldException;

public class TestSortingBuilder {

	@Test
	public void testBuildSorting() throws InvalidFieldException, CoGrouperException {
		SortingBuilder builder = new SortingBuilder();
		Sorting sorting = builder.add("url", SortOrder.ASC).add("date", SortOrder.DESC).addSourceId(SortOrder.ASC)
			.secondarySort(1).add("name", SortOrder.ASC).add("surname", SortOrder.DESC)
			.secondarySort(2).add("taste", SortOrder.ASC)
			.buildSorting();
		Assert.assertNotNull(sorting);
		Assert.assertEquals(SortOrder.ASC, sorting.getSortCriteria().getSortElementByFieldName("url").getSortOrder());
		Assert.assertEquals(SortOrder.DESC, sorting.getSortCriteria().getSortElementByFieldName("date").getSortOrder());
		Assert.assertEquals(SortOrder.ASC, sorting.getSpecificCriteriaByName(1).getSortElementByFieldName("name").getSortOrder());
		Assert.assertEquals(SortOrder.DESC, sorting.getSpecificCriteriaByName(1).getSortElementByFieldName("surname").getSortOrder());
		Assert.assertEquals(SortOrder.ASC, sorting.getSpecificCriteriaByName(2).getSortElementByFieldName("taste").getSortOrder());		
	}
	
	@Test(expected = CoGrouperException.class)
	public void testSourceIdMissingWithSpecificSortings() throws InvalidFieldException, CoGrouperException, IOException {
		new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.ASC)
			.secondarySort(1).add("foo", SortOrder.DESC)
			.secondarySort(2).add("name", SortOrder.DESC)
			.buildSorting();
	}
}
