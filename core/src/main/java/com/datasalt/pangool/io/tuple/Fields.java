package com.datasalt.pangool.io.tuple;

import java.util.ArrayList;
import java.util.List;

import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Schema.InternalType;

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
				fields.add(new Field(fieldName, strToClass(fieldType)));
			}
			return fields;
		} catch(ClassNotFoundException e) {
			throw new CoGrouperException(e);
		}
	}
	
	public static Class<?> strToClass(String str) throws ClassNotFoundException {
		Class<?> clazz = null;
		for (InternalType iType : InternalType.values()) {
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
