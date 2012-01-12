package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.pangool.Schema.Field;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;

public class TestPangoolConfig {

	@Test
	public void testCommonOrderedSchema() throws CoGrouperException {
		PangoolConfig config = new PangoolConfig();

		config.addSchema(0, Schema.parse("url:string, date:long, fetched:long, content:string"));
		config.addSchema(1, Schema.parse("fetched:long, url:string, name:string"));
		config.setSorting(Sorting.parse("url asc, fetched desc"));
		config.setGroupByFields("url");
		config.build();
		
		Assert.assertEquals(Schema.parse("url:string, fetched:long").toString(), config.getCommonOrderedSchema().toString());
	}
	
	@Test
	public void testCommonOrderedSchemaWithSourceId() throws InvalidFieldException, CoGrouperException {
		PangoolConfig config = new PangoolConfig();

		config.addSchema(0, Schema.parse("url:string, date:long, fetched:long, content:string"));
		config.addSchema(1, Schema.parse("fetched:long, url:string, name:string"));
		
		config.setSorting(new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("fetched", SortOrder.DESC)
			.addSourceId(SortOrder.ASC)
			.buildSorting()
		);

		config.setGroupByFields("url");
		config.build();
		
		Assert.assertEquals(Schema.parse("url:string, fetched:long, " + Field.SOURCE_ID_FIELD + ":vint").toString(), config.getCommonOrderedSchema().toString());
	}
	
	@Test
	public void testParticularPartialOrderedSchemas() throws CoGrouperException {
		PangoolConfig config = new PangoolConfig();

		config.addSchema(0, Schema.parse("url:string, date:long, fetched:long, content:string"));
		config.addSchema(1, Schema.parse("fetched:long, url:string, name:string"));
		config.setSorting(Sorting.parse("url asc, fetched desc"));
		config.setGroupByFields("url");
		config.build();
		
		Map<Integer, Schema> partialOrderedSchemas = config.getParticularPartialOrderedSchemas();

		Assert.assertEquals(Schema.parse("content:string, date:long").toString(), partialOrderedSchemas.get(0).toString()); 
		Assert.assertEquals(Schema.parse("name:string").toString(), partialOrderedSchemas.get(1).toString());
	}
	
	@Test
	public void testSerDeEquality() throws JsonGenerationException, JsonMappingException, IOException, CoGrouperException, InvalidFieldException {
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
			.addSourceId(SortOrder.ASC)
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
		config.setCustomPartitionerFields("url");

		ObjectMapper mapper = new ObjectMapper();
		String jsonConfig = config.toStringAsJSON(mapper);		
		PangoolConfig config2 = PangoolConfig.fromJSON(jsonConfig, mapper);

		Assert.assertEquals(jsonConfig, config2.toStringAsJSON(mapper));
	}
}
