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

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangolin.grouper.SortCriteria.SortElement;
import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;
import com.datasalt.pangolin.grouper.io.tuple.SortComparator;

public class TestSortCriteria {

	@Test
	public void test() throws GrouperException{
		
		SortCriteria criteria = SortCriteria.parse("name asc , caca using " + SortComparator.class.getName() + " desc");
		SortElement[] sortElement = criteria.getSortElements();
		assertEquals(2,sortElement.length);
		assertEquals("name",sortElement[0].getFieldName());
		assertEquals(SortOrder.ASC,sortElement[0].getSortOrder());
		assertNull(sortElement[0].getComparator());
		
		assertEquals("caca",sortElement[1].getFieldName());
		assertEquals(SortOrder.DESC,sortElement[1].getSortOrder());
		assertEquals(SortComparator.class,sortElement[1].getComparator());
		
		
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
