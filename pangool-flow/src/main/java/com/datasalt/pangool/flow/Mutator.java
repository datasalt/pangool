package com.datasalt.pangool.flow;

import java.util.ArrayList;
import java.util.List;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;

public class Mutator {
	
	public static int COUNTER = 0;
	
	public static Schema subSetOf(Schema schema, String... subSetFields) {
		List<Field> newSchema = new ArrayList<Field>();
		for(String subSetField: subSetFields) {
			newSchema.add(schema.getField(subSetField));
		}
		COUNTER++;
		return new Schema("subSetSchema" + COUNTER, newSchema);
	}
	
	public static Schema superSetOf(Schema schema, Field... newFields) {
		List<Field> newSchema = new ArrayList<Field>();
		newSchema.addAll(schema.getFields());
		for(Field newField: newFields) {
			newSchema.add(newField);
		}
		COUNTER++;
		return new Schema("superSetSchema" + COUNTER, newSchema);		
	}
	
	public static Schema jointSchema(Schema leftSchema, Schema rightSchema) {
		List<Field> newSchema = new ArrayList<Field>();
		for(Field field: leftSchema.getFields()) {
			newSchema.add(field);
		}
		for(Field field: rightSchema.getFields()) {
			if(!leftSchema.containsField(field.getName())) {
				newSchema.add(field);
			}
		}
		return new Schema("jointSchema" + COUNTER, newSchema);
	}
}
