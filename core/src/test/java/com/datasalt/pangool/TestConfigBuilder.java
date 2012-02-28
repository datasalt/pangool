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
package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datasalt.pangool.io.tuple.Fields;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.sorting.Criteria;
import com.datasalt.pangool.tuplemr.sorting.SortBy;
import com.datasalt.pangool.tuplemr.sorting.Criteria.Order;
import com.datasalt.pangool.tuplemr.sorting.Criteria.SortElement;


/**
 * 
 * Drums of tests tochísima ( Batería de tests)
 *
 */
public class TestConfigBuilder extends BaseTest{

	@Test
	public void testCorrectMinimal() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("a");
		TupleMRConfig conf = b.buildConf();
		conf.getSerializationInfo();
	}
	
	@Test
	public void testCorrect2() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a",Order.ASC).addSourceOrder(Order.DESC).add("b",Order.DESC));
		TupleMRConfig conf = b.buildConf();
		conf.getSerializationInfo();
	}
	
	@Test
	public void testCommonSortByToCriteria() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8,c:utf8,blabla:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,c:utf8,b:utf8,bloblo:utf8")));
		b.setGroupByFields("c","b");
		b.setOrderBy(new SortBy().add("b",Order.ASC).add("c",Order.DESC).addSourceOrder(Order.DESC).add("a",Order.DESC));
		b.setSecondaryOrderBy("schema1",new SortBy().add("blabla", Order.DESC));
		TupleMRConfig config = b.buildConf();
		config.getSerializationInfo();
		{
			List<SortElement> expectedCommon = new ArrayList<SortElement>();
			expectedCommon.add(new SortElement("b",Order.ASC));
			expectedCommon.add(new SortElement("c",Order.DESC));
			Assert.assertEquals(new Criteria(expectedCommon),config.getCommonCriteria());
		}
		{
			List<SortElement> expectedSchema1 = new ArrayList<SortElement>();
			expectedSchema1.add(new SortElement("a",Order.DESC));
			expectedSchema1.add(new SortElement("blabla",Order.DESC));
			Assert.assertEquals(new Criteria(expectedSchema1),config.getSecondarySortBys().get(0));
		}
		{
			List<SortElement> expectedSchema2 = new ArrayList<SortElement>();
			expectedSchema2.add(new SortElement("a",Order.DESC));
			Assert.assertEquals(new Criteria(expectedSchema2),config.getSecondarySortBys().get(1));
		}
		
	}
	
	@Test
	public void testCommonOrderGeneratedImplicitlyFromGroupFields() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8,c:utf8,blabla:utf8")));
		b.setGroupByFields("c","b");
		TupleMRConfig config = b.buildConf();
		config.getSerializationInfo();
		{
			List<SortElement> expectedCommon = new ArrayList<SortElement>();
			expectedCommon.add(new SortElement("c",Order.ASC));
			expectedCommon.add(new SortElement("b",Order.ASC));
			Assert.assertEquals(new Criteria(expectedCommon),config.getCommonCriteria());
		}
	}
	
	@Test(expected=TupleMRException.class)
	public void testSortFieldWithDifferentTypes1() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:boolean")));
		b.setGroupByFields("a");
		//not allowed to sort in common order by a field that has different types even after source order
		//it can be confusing
		b.setOrderBy(new SortBy().add("a",Order.ASC).addSourceOrder(Order.DESC).add("b",Order.DESC)); 
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testSortFieldWithDifferentTypes2() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:boolean")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a",Order.ASC).add("b",Order.DESC));
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testGroupByFieldWithDifferentTypes() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:boolean")));
		b.setGroupByFields("b","a");
		b.buildConf();
	}
	
	@Test (expected=TupleMRException.class)
	public void testRepeatedSchemas() throws TupleMRException{
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("c:int,b:utf8")));
		b.setGroupByFields("b");
		b.buildConf();
	}
	
	@Test (expected=TupleMRException.class)
	public void testGroupByInvalidField() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("c");
		b.buildConf();
	}
	
	@Test (expected=TupleMRException.class)
	public void testCommonOrderPrefixGroupBy() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("b",Order.ASC));
		b.buildConf();
	}
	
	@Test (expected=TupleMRException.class)
	public void testCommonOrderPrefixGroupBy2() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8,c:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:utf8,d:utf8")));
		b.setGroupByFields("a","b");
		b.setOrderBy(new SortBy().add("b",Order.ASC).addSourceOrder(Order.DESC).add("a",Order.DESC));
		b.buildConf();
	}
	
	
	@Test (expected=IllegalArgumentException.class)
	public void testNotRepeatedFieldsInSortBy() {
		new SortBy().add("foo",Order.ASC).add("foo",Order.DESC);
	}
	
	@Test (expected=IllegalStateException.class)
	public void testNotRepeatedSourceOrderInSortBy() throws TupleMRException {
		new SortBy().add("foo",Order.DESC).addSourceOrder(Order.ASC).add("bar",Order.DESC).addSourceOrder(Order.DESC);
	}
	
	@Test (expected=TupleMRException.class)
	public void testNotAllowedSourceOrderInOneSource() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a",Order.ASC).addSourceOrder(Order.DESC));
		b.buildConf();
	}
	
	@Test (expected=TupleMRException.class)
	public void testNotAllowedSourceOrderInSecondaryOrder() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("c:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1", new SortBy().add("a",Order.DESC).addSourceOrder(Order.DESC)); //this is incorrect
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testSecondaryOrderExistingSource() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("c:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("invented_schema", new SortBy().add("a",Order.DESC).addSourceOrder(Order.DESC));
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testSecondaryOrderNotNull() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("c:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1", null);
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testSecondaryOrderNotEmpty() throws TupleMRException{
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("c:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1", new SortBy());
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testCommonOrderNotNull() throws TupleMRException{
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(null);
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testCommonOrderNotEmpty() throws TupleMRException{
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy());
		b.buildConf();
	}
	
	
	
	@Test(expected=TupleMRException.class)
	public void testFieldsRepeatedInCommonAndSecondaryOrder() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1", new SortBy().add("b",Order.ASC));
		b.buildConf();
	}
	
	
	
	@Test(expected=TupleMRException.class)
	public void testNeedToDeclareCommonOrderWhenSecondary() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("b");
		b.setSecondaryOrderBy("schema1", new SortBy().add("a",Order.ASC));
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testSourceOrderPresentInCommonWhenSecondarySet() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC));
		b.setSecondaryOrderBy("schema1", new SortBy().add("a",Order.ASC));
		b.buildConf();
		
	}
	
	@Test(expected=TupleMRException.class)
	public void testRollUpCantBeNull() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC));
		b.setRollupFrom(null);
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testRollupPrefixGroupBy() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC));
		b.setRollupFrom(null);
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testRollupNeedsExplicitSortBy() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("b","a");
		b.setRollupFrom("a");
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testSpecificSortingNotAllowedWithOneSource() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a", Order.ASC));
		b.setSecondaryOrderBy("schema1", new SortBy().add("b", Order.ASC));		
		b.buildConf();		
	}
	
	@Test(expected=TupleMRException.class)
	public void testCustomPartitionFieldsNotEmpty() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a", Order.ASC));
		b.setCustomPartitionFields();		
		b.buildConf();	
	}
	
	@Test(expected=TupleMRException.class)
	public void testCustomPartitionFieldsNotNull() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a", Order.ASC));
		String [] array = null;
		b.setCustomPartitionFields(array);		
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testCustomPartitionFieldsPresentInAllSources() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:utf8,c:long")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a", Order.ASC));
		b.setCustomPartitionFields("c");		
		b.buildConf();
	}
	
	@Test(expected=TupleMRException.class)
	public void testCustomPartitionFieldsPresentWithSameType() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("a:int,b:long")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a", Order.ASC));
		b.setCustomPartitionFields("b");		
		b.buildConf();
	}
	
	@Test
	public void testCustomPartition() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("b:utf8,a:int")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a", Order.ASC));
		b.setCustomPartitionFields("b");		
		TupleMRConfig config = b.buildConf();
		System.out.println(config);
		SerializationInfo serInfo = config.getSerializationInfo();
		int[] indexes0 = serInfo.getFieldsToPartition(0);
		int[] indexes1 = serInfo.getFieldsToPartition(1);
		Assert.assertArrayEquals(new int[]{1}, indexes0);
		Assert.assertArrayEquals(new int[]{0}, indexes1);
	}
	
	
	
	@SuppressWarnings("unused")
	@Ignore
	@Test(expected=UnsupportedOperationException.class)
	public void testNotMutableConfig() throws TupleMRException {
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("a:int,b:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("b:utf8,a:int")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a", Order.ASC));
		b.setCustomPartitionFields("b");		
		TupleMRConfig config = b.buildConf(); //TODO
	}
	
}
