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
package com.datasalt.pangool.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Assert;
import org.junit.Before;

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.cogroup.TupleMRConfig;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.TupleMRConfigBuilder;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.io.tuple.Fields;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Tuple;

public abstract class ComparatorsBaseTest extends BaseTest{
	
	private Schema schema1;
	private Schema schema2;
	
	@Before
	public void initSchemas() throws TupleMRException{
		this.schema1 =  new Schema("schema1",Fields.parse("intField:int, strField:string,booleanField:boolean"));
		this.schema2 = new Schema("schema2",Fields.parse("longField:long,booleanField:boolean, intField:int"));
		
	}
	
	protected void setConf(SortComparator comparator) throws TupleMRException, JsonGenerationException, JsonMappingException, IOException {
		
		Configuration conf = new Configuration();
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(schema1);
		b.addIntermediateSchema(schema2);
		b.setGroupByFields("booleanField", "intField");
		b.setOrderBy(new SortBy().add("booleanField",Order.ASC).add("intField",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1",new SortBy().add("strField",Order.DESC));
		b.setSecondaryOrderBy("schema2",new SortBy().add("longField",Order.DESC));
		TupleMRConfig config = b.buildConf();
		TupleMRConfig.set(config, conf);
		comparator.setConf(conf);
	}
	
	protected Tuple getTuple1(boolean booleanValue, int intValue, String strValue) {
		Tuple tuple = new Tuple(schema1);
		tuple.set("booleanField", booleanValue);
		tuple.set("intField", intValue);
		tuple.set("strField", strValue);
		return tuple;
	}
	
	protected Tuple getTuple2(boolean booleanValue, int intValue, long longValue) {
		Tuple tuple = new Tuple(schema2);
		tuple.set("booleanField", booleanValue);
		tuple.set("intField", intValue);
		tuple.set("longField", longValue);
		return tuple;
	}
	
	protected static void assertPositive(RawComparator<ITuple> comp,ITuple t1,ITuple t2){
		assertPositive(comp.compare(t1,t2));
	}
	
	protected static void assertNegative(RawComparator<ITuple> comp,ITuple t1,ITuple t2){
		assertNegative(comp.compare(t1,t2));
	}
	
	protected static void assertPositive(int n){
		Assert.assertTrue(n > 0);
	}
	
	protected static void assertNegative(int n){
		Assert.assertTrue(n < 0);
	}

}
