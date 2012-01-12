package com.datasalt.pangolin.pangool;

import java.io.IOException;

import junit.framework.Assert;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;

public class TestPangoolConfig {

	@Test
	public void test() throws JsonGenerationException, JsonMappingException, IOException, CoGrouperException, InvalidFieldException {
		PangoolConfig config = new PangoolConfig();

		SchemaBuilder builder1 = new SchemaBuilder();
		builder1
			.add("url", String.class)
			.add("date", Long.class)
			.add("content", String.class);

		SchemaBuilder builder2 = new SchemaBuilder();
		builder2
			.add("url", String.class)
			.add("date", Long.class)
			.add("name", String.class);

		SortingBuilder builder = new SortingBuilder();
		Sorting sorting = 
			builder
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.DESC)
			.secondarySort(1)
				.add("content", SortOrder.ASC)
			.secondarySort(2)
				.add("name", SortOrder.ASC)
			.buildSorting();

		config.addSchema(1, builder1.createSchema());
		config.addSchema(2, builder2.createSchema());
		config.setSorting(sorting);
		config.setRollupFrom("url");
		config.setGroupByFields("url", "date");

		ObjectMapper mapper = new ObjectMapper();
		String jsonConfig = config.toStringAsJSON(mapper);
		PangoolConfig config2 = PangoolConfig.fromJSON(jsonConfig, mapper);

		Assert.assertEquals(jsonConfig, config2.toStringAsJSON(mapper));
	}
}
