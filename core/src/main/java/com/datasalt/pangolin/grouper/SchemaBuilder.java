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

import com.datasalt.pangolin.grouper.Schema.Field;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;

public class SchemaBuilder {

	private List<Field> fields = new ArrayList<Field>();
	
	public void addField(String fieldName,Class type) throws InvalidFieldException{
		if (fieldAlreadyExists(fieldName)){
			throw new InvalidFieldException("Field '" + fieldName + "' already exists");
		}
		
		if (type == null){
			throw new InvalidFieldException("Type for field '" + fieldName + "' can't be null");
		}
		
		fields.add(new Field(fieldName, type));
	}
	
	private boolean fieldAlreadyExists(String fieldName){
		for (Field field : fields){
			if (field.getName().equalsIgnoreCase(fieldName)){
				return true;
			}
		}
		return false;
	}
	
	public Schema createSchema(){
		Field[] fieldsArray = new Field[fields.size()];
		fields.toArray(fieldsArray);
		return new Schema(fieldsArray);
	}
}
