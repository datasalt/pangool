/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.tuplemr;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSerializationInfo {

	@Test
	public void testOneSource() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1", Fields
		    .parse("a:int,b:string,c:string,blabla:string,p:string")));
		
		b.setGroupByFields("c", "b");
		OrderBy commonOrderBy = new OrderBy();
		commonOrderBy.add("b", Order.ASC);
		commonOrderBy.add("c", Order.DESC);
		commonOrderBy.add("a", Order.DESC);
		b.setOrderBy(commonOrderBy);
		b.setRollupFrom("b");
		b.setCustomPartitionFields("p");
		TupleMRConfig config = b.buildConf();
		
		
		{
			List<SortElement> expectedCommon = new ArrayList<SortElement>();
			expectedCommon.add(new SortElement("b", Order.ASC, Criteria.NullOrder.NULLS_FIRST));
			expectedCommon.add(new SortElement("c", Order.DESC, Criteria.NullOrder.NULLS_FIRST));
			expectedCommon.add(new SortElement("a", Order.DESC, Criteria.NullOrder.NULLS_FIRST));
			assertEquals(new Criteria(expectedCommon), config.getCommonCriteria());
		}
		
//		{
//			//TODO this is fragile. there's ambiguity if fields in common schema must be like 
//			// original fields or aliases. In fact what matters is thats index translations 
//      tables are ok.
//			Schema commonSchema = serInfo.getCommonSchema();
//			Schema expectedCommonSchema = 
//				new Schema(commonSchema.getName(),Fields.parse("b:string,c:string,a:int,blabla:string,px:string"));
//			Assert.assertEquals(expectedCommonSchema,commonSchema);
//		}
		SerializationInfo serInfo = config.getSerializationInfo();
		{
			Schema groupSchema = serInfo.getGroupSchema();
			Schema expectedGroupSchema = 
				new Schema(groupSchema.getName(),Fields.parse("b:string,c:string"));
			assertEquals(expectedGroupSchema, groupSchema);
		}
		Assert.assertArrayEquals(new int[]{1,2,0,3,4},serInfo.getCommonSchemaIndexTranslation(0));
		Assert.assertArrayEquals(new int[]{4},serInfo.getFieldsToPartition(0));
		Assert.assertArrayEquals(new int[]{1,2},serInfo.getGroupSchemaIndexTranslation(0));

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
			aliases1.add("a","ax");
			aliases1.add("b","bx");
			aliases1.add("c","cx");
			aliases1.add("blabla","blablax");
			aliases1.add("p","px");
			b.setFieldAliases("schema1", aliases1);
		}
		{
			Aliases aliases2 = new Aliases();
			aliases2.add("a","ay");
			aliases2.add("b","by");
			aliases2.add("c","cy");
			
			aliases2.add("bloblo","blobloy");
			b.setFieldAliases("schema2", aliases2);
		}
		
		b.setGroupByFields("c", "b");
		
		OrderBy commonOrderBy = new OrderBy();
		commonOrderBy.add("b", Order.ASC);
		commonOrderBy.add("c", Order.DESC);
		commonOrderBy.addSchemaOrder(Order.DESC);
		commonOrderBy.add("a", Order.DESC);
		b.setOrderBy(commonOrderBy);
		b.setRollupFrom("b");
		b.setSpecificOrderBy("schema1", new OrderBy().add("blabla", Order.DESC, Criteria.NullOrder.NULLS_LAST));
		b.setSpecificOrderBy("schema2", new OrderBy().add("bloblo", Order.DESC));
		b.setCustomPartitionFields("p");
		TupleMRConfig config = b.buildConf();
		SerializationInfo serInfo = config.getSerializationInfo();
		
		{
			List<SortElement> expectedCommon = new ArrayList<SortElement>();
			expectedCommon.add(new SortElement("b", Order.ASC, Criteria.NullOrder.NULLS_FIRST));
			expectedCommon.add(new SortElement("c", Order.DESC, Criteria.NullOrder.NULLS_FIRST));
			assertEquals(new Criteria(expectedCommon), config.getCommonCriteria());
		}
		{
			List<SortElement> expectedSchema1 = new ArrayList<SortElement>();
			expectedSchema1.add(new SortElement("a", Order.DESC, Criteria.NullOrder.NULLS_FIRST));
			expectedSchema1.add(new SortElement("blabla", Order.DESC, Criteria.NullOrder.NULLS_LAST));
			assertEquals(new Criteria(expectedSchema1), config.getSpecificOrderBys()
          .get(0));
		}
		{
			List<SortElement> expectedSchema2 = new ArrayList<SortElement>();
			expectedSchema2.add(new SortElement("a", Order.DESC, Criteria.NullOrder.NULLS_FIRST));
			expectedSchema2.add(new SortElement("bloblo", Order.DESC, Criteria.NullOrder.NULLS_FIRST));
			assertEquals(new Criteria(expectedSchema2), config.getSpecificOrderBys()
          .get(1));
		}
		
		{
		Schema groupSchema = serInfo.getGroupSchema();
		Schema expectedGroupSchema = 
				new Schema(groupSchema.getName(),Fields.parse("b:string,c:string"));
		assertEquals(expectedGroupSchema, groupSchema);
		}
		{
		Schema commonSchema = serInfo.getCommonSchema();
		Schema expectedCommonSchema = 
				new Schema(commonSchema.getName(),Fields.parse("b:string,c:string"));
		assertEquals(expectedCommonSchema, commonSchema);
		}
//		{
//		Schema specificSchema1 = serInfo.getSpecificSchema(0);
//		Schema expectedSpecificSchema1 = 
//				new Schema(specificSchema1.getName(),Fields.parse("a:int,blabla:string,px:string"));
//		Assert.assertEquals(expectedSpecificSchema1,specificSchema1);
//		}
//		{
//		Schema specificSchema2 = serInfo.getSpecificSchema(1);
//		Schema expectedSpecificSchema2 = 
//				new Schema(specificSchema2.getName(),Fields.parse("a:int,p:string,blobloy:string"));
//		Assert.assertEquals(expectedSpecificSchema2,specificSchema2);
//		}
		Assert.assertArrayEquals(new int[]{1,2},serInfo.getCommonSchemaIndexTranslation(0));
		Assert.assertArrayEquals(new int[]{2,1},serInfo.getCommonSchemaIndexTranslation(1));
		
		Assert.assertArrayEquals(new int[]{1,2},serInfo.getGroupSchemaIndexTranslation(0));
		Assert.assertArrayEquals(new int[]{2,1},serInfo.getGroupSchemaIndexTranslation(1));
		
		Assert.assertArrayEquals(new int[]{4},serInfo.getFieldsToPartition(0));
		Assert.assertArrayEquals(new int[]{3},serInfo.getFieldsToPartition(1));
		
		Assert.assertArrayEquals(new int[]{0,3,4},serInfo.getSpecificSchemaIndexTranslation(0));
		Assert.assertArrayEquals(new int[]{0,4,3},serInfo.getSpecificSchemaIndexTranslation(1));

	}

  /**
   * Checks that the method {@link SerializationInfo#checkFieldInAllSchemas(String)} is
   * selecting properly nullable fields when several (nullable and non nullable) are
   * present.
   * @throws TupleMRException
   */
  @Test
  public void testCheckFieldInAllSchemas() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields
        .parse("ax:int,bx:string,cx:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields
        .parse("ay:int,cy:string,by:string")));
    b.addIntermediateSchema(new Schema("schema3", Fields
        .parse("a:int?,c:string,b:string?")));
    {
      Aliases aliases1 = new Aliases();
      aliases1.add("a","ax");
      aliases1.add("b","bx");
      aliases1.add("c","cx");
      b.setFieldAliases("schema1", aliases1);
    }
    {
      Aliases aliases2 = new Aliases();
      aliases2.add("a","ay");
      aliases2.add("b","by");
      aliases2.add("c","cy");
      b.setFieldAliases("schema2", aliases2);
    }

    b.setGroupByFields("b", "c");
    OrderBy commonOrderBy = new OrderBy();
    commonOrderBy.add("b", Order.ASC);
    commonOrderBy.add("c", Order.DESC);
    commonOrderBy.addSchemaOrder(Order.DESC);
    commonOrderBy.add("a", Order.DESC);
    b.setOrderBy(commonOrderBy);

    TupleMRConfig config = b.buildConf();
    SerializationInfo serInfo = config.getSerializationInfo();
    Schema common = serInfo.getCommonSchema();
    System.out.println(common);
    Schema.Field field = common.getField(0);
    assertEquals("b", field.getName());
    Assert.assertTrue(field.isNullable());
    field = common.getField(1);
    assertEquals("c", field.getName());
    assertFalse(field.isNullable());

    for (Schema schema : serInfo.getSpecificSchemas()) {
      assertEquals("a", schema.getField(0).getName());
      assertEquals(false, schema.getField(0).isNullable());
    }
  }
	
}

