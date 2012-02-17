package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Criteria.SortElement;


/**
 * 
 * Drums of tests tochísima ( Batería de tests)
 *
 */
public class TestConfigBuilder extends BaseTest{

	@Test
	public void testCorrectMinimal() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("a");
		CoGrouperConfig conf = b.buildConf();
		conf.getSerializationInfo();
	}
	
	@Test
	public void testCorrect2() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:string")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a",Order.ASC).addSourceOrder(Order.DESC).add("b",Order.DESC));
		CoGrouperConfig conf = b.buildConf();
		conf.getSerializationInfo();
	}
	
	@Test
	public void testCommonSortByToCriteria() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string,c:string,blabla:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,c:string,b:string,bloblo:string")));
		b.setGroupByFields("c","b");
		b.setOrderBy(new SortBy().add("b",Order.ASC).add("c",Order.DESC).addSourceOrder(Order.DESC).add("a",Order.DESC));
		b.setSecondaryOrderBy("schema1",new SortBy().add("blabla", Order.DESC));
		CoGrouperConfig config = b.buildConf();
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
	public void testCommonOrderGeneratedImplicitlyFromGroupFields() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string,c:string,blabla:string")));
		b.setGroupByFields("c","b");
		CoGrouperConfig config = b.buildConf();
		config.getSerializationInfo();
		{
			List<SortElement> expectedCommon = new ArrayList<SortElement>();
			expectedCommon.add(new SortElement("c",Order.ASC));
			expectedCommon.add(new SortElement("b",Order.ASC));
			Assert.assertEquals(new Criteria(expectedCommon),config.getCommonCriteria());
		}
	}
	
	@Test(expected=CoGrouperException.class)
	public void testSortFieldWithDifferentTypes1() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:boolean")));
		b.setGroupByFields("a");
		//not allowed to sort in common order by a field that has different types even after source order
		//it can be confusing
		b.setOrderBy(new SortBy().add("a",Order.ASC).addSourceOrder(Order.DESC).add("b",Order.DESC)); 
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testSortFieldWithDifferentTypes2() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:boolean")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a",Order.ASC).add("b",Order.DESC));
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testGroupByFieldWithDifferentTypes() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:boolean")));
		b.setGroupByFields("b","a");
		b.buildConf();
	}
	
	@Test (expected=CoGrouperException.class)
	public void testRepeatedSchemas() throws CoGrouperException{
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema1",Fields.parse("c:int,b:string")));
		b.setGroupByFields("a");
		b.buildConf();
	}
	
	@Test (expected=CoGrouperException.class)
	public void testGroupByInvalidField() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("c");
		b.buildConf();
	}
	
	@Test (expected=CoGrouperException.class)
	public void testCommonOrderPrefixGroupBy() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("b",Order.ASC));
		b.buildConf();
	}
	
	@Test (expected=CoGrouperException.class)
	public void testCommonOrderPrefixGroupBy2() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string,c:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:string,d:string")));
		b.setGroupByFields("a","b");
		b.setOrderBy(new SortBy().add("b",Order.ASC).addSourceOrder(Order.DESC).add("a",Order.DESC));
		b.buildConf();
	}
	
	
	@Test (expected=IllegalArgumentException.class)
	public void testNotRepeatedFieldsInSortBy() {
		new SortBy().add("foo",Order.ASC).add("foo",Order.DESC);
	}
	
	@Test (expected=IllegalStateException.class)
	public void testNotRepeatedSourceOrderInSortBy() throws CoGrouperException {
		new SortBy().add("foo",Order.DESC).addSourceOrder(Order.ASC).add("bar",Order.DESC).addSourceOrder(Order.DESC);
	}
	
	@Test (expected=CoGrouperException.class)
	public void testNotAllowedSourceOrderInOneSource() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a",Order.ASC).addSourceOrder(Order.DESC));
		b.buildConf();
	}
	
	@Test (expected=CoGrouperException.class)
	public void testNotAllowedSourceOrderInSecondaryOrder() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("c:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1", new SortBy().add("a",Order.DESC).addSourceOrder(Order.DESC)); //this is incorrect
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testSecondaryOrderExistingSource() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("c:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("invented_schema", new SortBy().add("a",Order.DESC).addSourceOrder(Order.DESC));
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testSecondaryOrderNotNull() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("c:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1", null);
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testSecondaryOrderNotEmpty() throws CoGrouperException{
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("c:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1", new SortBy());
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testCommonOrderNotNull() throws CoGrouperException{
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(null);
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testCommonOrderNotEmpty() throws CoGrouperException{
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy());
		b.buildConf();
	}
	
	
	
	@Test(expected=CoGrouperException.class)
	public void testFieldsRepeatedInCommonAndSecondaryOrder() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1", new SortBy().add("b",Order.ASC));
		b.buildConf();
	}
	
	
	
	@Test(expected=CoGrouperException.class)
	public void testNeedToDeclareCommonOrderWhenSecondary() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:string")));
		b.setGroupByFields("b");
		b.setSecondaryOrderBy("schema1", new SortBy().add("a",Order.ASC));
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testSourceOrderPresentInCommonWhenSecondarySet() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC));
		b.setSecondaryOrderBy("schema1", new SortBy().add("a",Order.ASC));
		b.buildConf();
		
	}
	
	@Test(expected=CoGrouperException.class)
	public void testRollUpCantBeNull() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("a:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC));
		b.setRollupFrom(null);
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testRollupPrefixGroupBy() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("b");
		b.setOrderBy(new SortBy().add("b",Order.DESC));
		b.setRollupFrom(null);
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testRollupNeedsExplicitSortBy() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("b","a");
		b.setRollupFrom("a");
		b.buildConf();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testSpecificSortingNotAllowedWithOneSource() throws CoGrouperException {
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("a:int,b:string")));
		b.setGroupByFields("a");
		b.setOrderBy(new SortBy().add("a", Order.ASC));
		b.setSecondaryOrderBy("schema1", new SortBy().add("b", Order.ASC));		
		b.buildConf();		
	}
	
//	@Test(expected=CoGrouperException.class)
//	public void testNotMutableConfig(){
//		//TODO
//	}
	
}
