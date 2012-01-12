package com.datasalt.pangolin.pangool;

import java.util.ArrayList;
import java.util.List;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.pangool.Schema.Field;

/**
 * Builds one inmutable {@link Schema} instance.
 * 
 * @author pere
 *
 */
public class SchemaBuilder {

	private List<Field> fields = new ArrayList<Field>();

	public SchemaBuilder add(String fieldName, Class<?> type) throws InvalidFieldException {
		if(fieldAlreadyExists(fieldName)) {
			throw new InvalidFieldException("Field '" + fieldName + "' already exists");
		}

		if(type == null) {
			throw new InvalidFieldException("Type for field '" + fieldName + "' can't be null");
		}

		if(fieldName.equals(Schema.Field.SOURCE_ID_FIELD)) {
			throw new InvalidFieldException("Can't define a field with reserved name: " + Schema.Field.SOURCE_ID_FIELD);
		}
		
		fields.add(new Field(fieldName, type));
		return this;
	}

	private boolean fieldAlreadyExists(String fieldName) {
		for(Field field : fields) {
			if(field.getName().equals(fieldName)) {
				return true;
			}
		}
		return false;
	}

	public Schema createSchema() {
		return new Schema(fields);
	}
}
