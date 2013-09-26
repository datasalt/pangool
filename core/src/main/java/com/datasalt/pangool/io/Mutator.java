package com.datasalt.pangool.io;

import java.util.ArrayList;
import java.util.List;

import com.datasalt.pangool.io.Schema.Field;

/**
 * Miscellaneous utilities for mutating Pangool schemas.
 * One can use these to easily create supersets or subsets of a Schema, or joint Schemas between
 * two Schemas. The details of each method is documented below:
 */
public class Mutator {
	
	public static int COUNTER = 0;

	/**
	 * Creates a subset of the input Schema exactly with the fields whose names are specified.
	 * The name of the schema is auto-generated with a static counter.
	 */
	public static Schema subSetOf(Schema schema, String... subSetFields) {
		return subSetOf(schema, "subSetSchema" + (COUNTER++), subSetFields);
	}

	/**
	 * Creates a subset of the input Schema exactly with the fields whose names are specified.
	 * The name of the schema is also specified as a parameter.
	 */
	public static Schema subSetOf(Schema schema, String newName, String... subSetFields) {
		List<Field> newSchema = new ArrayList<Field>();
		for(String subSetField: subSetFields) {
			newSchema.add(schema.getField(subSetField));
		}
		return new Schema(newName, newSchema);
	}
	
	/**
	 * Creates a superset of the input Schema, taking all the Fields in the input schema
	 * and adding some new ones. The new fields are fully specified in a Field class. 
	 * The name of the schema is auto-generated with a static counter.
	 */
	public static Schema superSetOf(Schema schema, Field... newFields) {
		return superSetOf(schema, "superSetSchema" + (COUNTER++), newFields);
	}
	
	/**
	 * Creates a superset of the input Schema, taking all the Fields in the input schema
	 * and adding some new ones. The new fields are fully specified in a Field class. 
	 * The name of the schema is also specified as a parameter.
	 */
	public static Schema superSetOf(Schema schema, String newName, Field... newFields) {
		List<Field> newSchema = new ArrayList<Field>();
		newSchema.addAll(schema.getFields());
		for(Field newField: newFields) {
			newSchema.add(newField);
		}
		return new Schema(newName, newSchema);		
	}
	
	/**
	 * Creates a joint schema between two Schemas. All Fields from both schema are deduplicated
	 * and combined into a single Schema. The left Schema has priority so if both Schemas have
	 * the same Field with the same name but different Types, the Type from the left Schema will be
	 * taken.
	 * <p>
	 * The name of the schema is auto-generated with a static counter.
	 */
	public static Schema jointSchema(Schema leftSchema, Schema rightSchema) {
		return jointSchema(leftSchema, rightSchema, "jointSchema" + (COUNTER++));
	}
	
	/**
	 * Creates a joint schema between two Schemas. All Fields from both schema are deduplicated
	 * and combined into a single Schema. The left Schema has priority so if both Schemas have
	 * the same Field with the same name but different Types, the Type from the left Schema will be
	 * taken.
	 * <p>
	 * The name of the schema is also specified as a parameter.
	 */
	public static Schema jointSchema(Schema leftSchema, Schema rightSchema, String newName) {
		List<Field> newSchema = new ArrayList<Field>();
		for(Field field: leftSchema.getFields()) {
			newSchema.add(field);
		}
		for(Field field: rightSchema.getFields()) {
			if(!leftSchema.containsField(field.getName())) {
				newSchema.add(field);
			}
		}
		return new Schema(newName, newSchema);
	}
}

