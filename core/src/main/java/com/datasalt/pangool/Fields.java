package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.List;

import com.datasalt.pangool.Schema.Field;

public class Fields {

	public static List<Field> parse(String serialized) throws CoGrouperException{
		
		try {
			if(serialized == null || serialized.isEmpty()) {
				return null;
			}
			String[] fieldsStr = serialized.split(",");
			List<Field> fields = new ArrayList<Field>();
			for(String field : fieldsStr) {
				String[] nameType = field.split(":");
				if(nameType.length != 2) {
					throw new CoGrouperException("Incorrect fields description " + serialized);
				}
				String fieldName = nameType[0].trim();
				String fieldType = nameType[1].trim();
				fields.add(new Field(fieldName, Schema.strToClass(fieldType)));
			}
			return fields;
		} catch(ClassNotFoundException e) {
			throw new CoGrouperException(e);
		}
	}
	
	
}
