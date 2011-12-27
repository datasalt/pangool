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

import java.util.HashMap;
import java.util.Map;

public class SchemaBuilder {

	private Map<Integer,FieldsDescription> fieldsDescriptionBySource = new HashMap<Integer,FieldsDescription>();
	private SortCriteria sortCriteria;
	
	public SchemaBuilder(){
		
	}
	
	public void setFieldsDescriptionForSource(int source,String fieldsDescription) throws GrouperException{
		FieldsDescription f = FieldsDescription.parse(fieldsDescription);
		fieldsDescriptionBySource.put(source,f);
	}
	
	public void setSortCriteria(String sortCriteria) throws GrouperException{
		this.sortCriteria = SortCriteria.parse(sortCriteria);
	}
	
	public Schema createSchema() throws GrouperException{
		  return new Schema(fieldsDescriptionBySource,sortCriteria);
	}
	
}
