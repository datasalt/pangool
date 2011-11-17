/**
 * Copyright [2011] [Datasalt Systems S.L.]
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

package com.datasalt.pangolin.grouper;

import java.util.List;

import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.datasalt.pangolin.grouper.SortCriteria.Sort;

public class TestSortCriteria {

	@Test
	public void test() throws GrouperException{
		
		SortCriteria criteria = SortCriteria.parse("name asc , caca DESC");
		String[] fieldNames = criteria.getFieldNames();
		assertEquals(2,fieldNames.length);
		assertEquals("name",fieldNames[0]);
		assertEquals(Sort.ASC,criteria.getSortByFieldName("name"));
		assertEquals("caca",fieldNames[1]);
		assertEquals(Sort.DESC,criteria.getSortByFieldName("caca"));
		
		
		try{
			SortCriteria.parse("name asc,name desc");
			Assert.fail();
		} catch(GrouperException e ){}
		
		try{
			SortCriteria.parse("name caca,age desc");
			Assert.fail();
		} catch(GrouperException e ){}
		
		try{
			SortCriteria.parse("name asc,NAME desc");
			Assert.fail();
		} catch(GrouperException e ){}
		
		try{
			SortCriteria.parse("name asc guachu ,NaME desc");
			Assert.fail();
		} catch(GrouperException e ){}
		
		
	}
}
