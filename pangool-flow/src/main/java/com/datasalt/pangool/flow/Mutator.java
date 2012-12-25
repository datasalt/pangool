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
package com.datasalt.pangool.flow;

import java.util.ArrayList;
import java.util.List;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;

/**
 * Miscellaneous utils for mutating Pangool schemas.
 */
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
