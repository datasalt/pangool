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
package com.datasalt.pangool.tuplemr.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field.FieldSerializer;
import com.datasalt.pangool.tuplemr.Criteria;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
/**
 * 
 * Tuple-based MapRed jobs binary group comparator. Used to group tuples 
 * according to {@link TupleMRConfigBuilder#setGroupByFields(String...)}
 *
 */
public class GroupComparator extends SortComparator {

	private Criteria groupCriteria;
	
	public GroupComparator(){}
	
	@Override
	public int compare(ITuple w1, ITuple w2) {
		int schemaId1 = tupleMRConf.getSchemaIdByName(w1.getSchema().getName());
		int schemaId2 = tupleMRConf.getSchemaIdByName(w2.getSchema().getName());
		int[] indexes1 = serInfo.getGroupSchemaIndexTranslation(schemaId1);
		int[] indexes2 = serInfo.getGroupSchemaIndexTranslation(schemaId2);
	  FieldSerializer[] serializers = serInfo.getGroupSchemaSerializers();
		return compare(w1.getSchema(), groupCriteria, w1, indexes1, w2, indexes2,serializers);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try{
		Schema groupSchema = serInfo.getGroupSchema();
		return compare(b1,s1,b2,s2,groupSchema,groupCriteria,offsets);
		} catch(IOException e){
			throw new RuntimeException(e);
		}
	}
		
	@Override
	public void setConf(Configuration conf) {
	  super.setConf(conf);
	  //THIS SHOULD BE IN SerInfo
		List<SortElement> sortElements = tupleMRConf.getCommonCriteria().getElements();
		int numGroupByFields = tupleMRConf.getGroupByFields().size();
		List<SortElement> groupSortElements = new ArrayList<SortElement>();
		groupSortElements.addAll(sortElements);
		groupSortElements = groupSortElements.subList(0,numGroupByFields);
		groupCriteria = new Criteria(groupSortElements);					
		TupleMRConfigBuilder.initializeComparators(conf, tupleMRConf);	  
	}
	
}
