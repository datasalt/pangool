package com.datasalt.pangool.tuplemr;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;

public class TestSerializationInfo {

	@Test
	public void testOneSource() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1", Fields
		    .parse("ax:int,bx:string,c:string,blabla:string,px:string")));
		{
			Aliases aliases1 = new Aliases();
			aliases1.addAlias("a","ax");
			aliases1.addAlias("b","bx");
			aliases1.addAlias("p","px");
			b.setFieldAliases("schema1", aliases1);
		}
		
		b.setGroupByFields("c", "b");
		OrderBy commonOrderBy = new OrderBy();
		commonOrderBy.add("b", Order.ASC);
		commonOrderBy.add("c", Order.DESC);
		commonOrderBy.add("a", Order.DESC);
		b.setOrderBy(commonOrderBy);
		b.setCustomPartitionFields("p");
		TupleMRConfig config = b.buildConf();
		SerializationInfo serInfo = config.getSerializationInfo();
		
		{
			List<SortElement> expectedCommon = new ArrayList<SortElement>();
			expectedCommon.add(new SortElement("b", Order.ASC));
			expectedCommon.add(new SortElement("c", Order.DESC));
			expectedCommon.add(new SortElement("a", Order.DESC));
			Assert.assertEquals(new Criteria(expectedCommon), config.getCommonCriteria());
		}
		
		
		{
			Schema groupSchema = serInfo.getGroupSchema();
			Schema expectedGroupSchema = 
				new Schema(groupSchema.getName(),Fields.parse("b:string,c:string"));
			Assert.assertEquals(expectedGroupSchema,groupSchema);
		}
		{
			//TODO this is fragile. there's ambiguity if fields in common schema must be like 
			// original fields or aliases. 
			Schema commonSchema = serInfo.getCommonSchema();
			Schema expectedCommonSchema = 
				new Schema(commonSchema.getName(),Fields.parse("b:string,c:string,a:int,blabla:string,px:string"));
			Assert.assertEquals(expectedCommonSchema,commonSchema);
		}
		
		int[] commonToSource1Indexes = serInfo.getCommonSchemaIndexTranslation(0);
		Assert.assertArrayEquals(new int[]{1,2,0,3,4},commonToSource1Indexes);

		int[] fieldsToPartition = serInfo.getFieldsToPartition(0);
		Assert.assertArrayEquals(new int[]{4},fieldsToPartition);

	}
	
	
	@Test
	public void testMultipleSources() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1", Fields
		    .parse("ax:int,bx:string,cx:string,blablax:string,px:string")));
		b.addIntermediateSchema(new Schema("schema2", Fields
		    .parse("ay:int,cy:string,by:string,p:string,blobloy:string")));
		{
			Aliases aliases1 = new Aliases();
			aliases1.addAlias("a","ax");
			aliases1.addAlias("b","bx");
			aliases1.addAlias("c","cx");
			aliases1.addAlias("blabla","blablax");
			aliases1.addAlias("p","px");
			b.setFieldAliases("schema1", aliases1);
		}
		{
			Aliases aliases2 = new Aliases();
			aliases2.addAlias("a","ay");
			aliases2.addAlias("b","by");
			aliases2.addAlias("c","cy");
			
			aliases2.addAlias("bloblo","blobloy");
			b.setFieldAliases("schema2", aliases2);
		}
		
		b.setGroupByFields("c", "b");
		
		OrderBy commonOrderBy = new OrderBy();
		commonOrderBy.add("b", Order.ASC);
		commonOrderBy.add("c", Order.DESC);
		commonOrderBy.addSchemaOrder(Order.DESC);
		commonOrderBy.add("a", Order.DESC);
		b.setOrderBy(commonOrderBy);
		b.setSpecificOrderBy("schema1", new OrderBy().add("blabla", Order.DESC));
		b.setCustomPartitionFields("p");
		TupleMRConfig config = b.buildConf();
		SerializationInfo serInfo = config.getSerializationInfo();
		
		
		{
			List<SortElement> expectedCommon = new ArrayList<SortElement>();
			expectedCommon.add(new SortElement("b", Order.ASC));
			expectedCommon.add(new SortElement("c", Order.DESC));
			Assert.assertEquals(new Criteria(expectedCommon), config.getCommonCriteria());
		}
		{
			List<SortElement> expectedSchema1 = new ArrayList<SortElement>();
			expectedSchema1.add(new SortElement("a", Order.DESC));
			expectedSchema1.add(new SortElement("blabla", Order.DESC));
			Assert.assertEquals(new Criteria(expectedSchema1), config.getSpecificOrderBys()
			    .get(0));
		}
		{
			List<SortElement> expectedSchema2 = new ArrayList<SortElement>();
			expectedSchema2.add(new SortElement("a", Order.DESC));
			Assert.assertEquals(new Criteria(expectedSchema2), config.getSpecificOrderBys()
			    .get(1));
		}
		
		{
		Schema groupSchema = serInfo.getGroupSchema();
		Schema expectedGroupSchema = 
				new Schema(groupSchema.getName(),Fields.parse("b:string,c:string"));
		Assert.assertEquals(expectedGroupSchema,groupSchema);
		}
		{
		Schema commonSchema = serInfo.getCommonSchema();
		Schema expectedCommonSchema = 
				new Schema(commonSchema.getName(),Fields.parse("b:string,c:string"));
		Assert.assertEquals(expectedCommonSchema,commonSchema);
		}
		{
		Schema specificSchema1 = serInfo.getSpecificSchema(0);
		Schema expectedSpecificSchema1 = 
				new Schema(specificSchema1.getName(),Fields.parse("ax:int,blablax:string,p:string"));
		Assert.assertEquals(expectedSpecificSchema1,specificSchema1);
		}
		{
		Schema specificSchema2 = serInfo.getSpecificSchema(1);
		Schema expectedSpecificSchema2 = 
				new Schema(specificSchema2.getName(),Fields.parse("ay:int,p:string,blobloy:string"));
		Assert.assertEquals(expectedSpecificSchema2,specificSchema2);
		}
		int[] commonToSource1Indexes = serInfo.getCommonSchemaIndexTranslation(0);
		
		//Assert.assertArrayEquals(new int[]{},new int[]);

	}
	
}
