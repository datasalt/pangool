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
package com.datasalt.pangool.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.tuplemr.TupleMRException;

public class Fields {

	private static Map<String,Type> strToType = new HashMap<String,Type>();
	
	static {
		strToType.put("int",Type.INT);
		strToType.put("utf8",Type.STRING);
		strToType.put("boolean",Type.BOOLEAN);
		strToType.put("double",Type.DOUBLE);
		strToType.put("float",Type.FLOAT);
		strToType.put("long",Type.LONG);
	}
	
	public static List<Field> parse(String serialized) throws TupleMRException{
		
		
			if(serialized == null || serialized.isEmpty()) {
				return null;
			}
			String[] fieldsStr = serialized.split(",");
			List<Field> fields = new ArrayList<Field>();
			for(String field : fieldsStr) {
				String[] nameType = field.split(":");
				if(nameType.length != 2) {
					throw new TupleMRException("Incorrect fields description " + serialized);
				}
				String fieldName = nameType[0].trim();
				String fieldType = nameType[1].trim();
				Type type = strToType.get(fieldType);
				try {
					if (type != null){
						fields.add(Field.create(fieldName,type));
					} else {
						Class objectClazz = Class.forName(fieldType);
						if (objectClazz.isEnum()){
							fields.add(Field.createEnum(fieldName,objectClazz));
						} else {
							fields.add(Field.createObject(fieldName,objectClazz));
						}
					}
				} catch(ClassNotFoundException e) {
					throw new TupleMRException("Type " + fieldType + " not a valid class name ",e);
				}
				
			}
			return fields;
		
	}
}
