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
package com.datasalt.pangool.io.tuple;

import java.util.ArrayList;
import java.util.List;

import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Schema.Type;

public class Fields {

	public static List<Field> parse(String serialized) throws TupleMRException{
		
		try {
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
				fields.add(new Field(fieldName, strToClass(fieldType)));
			}
			return fields;
		} catch(ClassNotFoundException e) {
			throw new TupleMRException(e);
		}
	}
	
	public static Class<?> strToClass(String str) throws ClassNotFoundException {
		Class<?> clazz = null;
		for (Type iType : Type.values()) {
			if (iType.getParsingString() != null && str.equals(iType.getParsingString())) {
				clazz = iType.getRepresentativeClass();
			}
		}
		if(clazz == null) {
			clazz = Class.forName(str);
		}
		return clazz;
	}
}
