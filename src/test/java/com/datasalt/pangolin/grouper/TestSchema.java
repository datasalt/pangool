package com.datasalt.pangolin.grouper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.datasalt.pangolin.commons.test.AbstractBaseTest;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;


public class TestSchema extends AbstractBaseTest{
	
	@Test
	public void test() throws GrouperException, JsonGenerationException, JsonMappingException, IOException{
		FieldsDescription description = FieldsDescription.parse("name:string,age:int");
		SortCriteria sortCriteria = SortCriteria.parse("name asc,age asc");
		
		Map<Integer,FieldsDescription> descriptionBySource = new HashMap<Integer,FieldsDescription>();
		descriptionBySource.put(0,description);
		Schema schema = new Schema(descriptionBySource,sortCriteria);
		String serialized = schema.toJson();
		System.out.println(schema);
		
		
		Schema schema2 = Schema.parseJson(serialized);
		System.out.println(schema2);
		
	}
	
	@Test
	public void testNotMatchingTypes()  throws GrouperException {
		
//		String format = "SOURCE t1: { user_id : int , name : string }\n"+
//										"SOURCE t2: { id:int, name : string}\n"
//		
//		Schema.parse("")
		
	}
}
