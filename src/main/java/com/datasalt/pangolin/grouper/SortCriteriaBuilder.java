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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.RawComparator;

import com.datasalt.pangolin.grouper.SortCriteria.SortElement;
import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;

public class SortCriteriaBuilder {

	private List<SortElement> fields = new ArrayList<SortElement>();
	
	public void addSortElement(String fieldName,SortOrder order,Class<? extends RawComparator> customComparator) throws InvalidFieldException{
		if (fieldAlreadyExists(fieldName)){
			throw new InvalidFieldException("Field '" + fieldName + "' already exists");
		}
		fields.add(new SortElement(fieldName, order,customComparator));
	}
	
	public void addSortElement(String fieldName,SortOrder order) throws InvalidFieldException{
		addSortElement(fieldName,order,null);
	}
		
	
	private boolean fieldAlreadyExists(String fieldName){
		for (SortElement field : fields){
			if (field.getName().equalsIgnoreCase(fieldName)){
				return true;
			}
		}
		return false;
	}
	
	public SortCriteria createSortCriteria(){
		SortElement[] fieldsArray = new SortElement[fields.size()];
		fields.toArray(fieldsArray);
		return new SortCriteria(fieldsArray);
	}
}
