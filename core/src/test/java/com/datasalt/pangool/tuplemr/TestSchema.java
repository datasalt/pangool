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
package com.datasalt.pangool.tuplemr;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;

public class TestSchema {

	@Test(expected=IllegalArgumentException.class)
	public void testNotRepeatedFields(){
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.STRING));
		fields.add(Field.create("foo", Type.STRING));
		
		new Schema("schema", fields);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testNameForSchemaNotNull(){
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.STRING));
		
		new Schema(null, fields);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIncorrectDefaultValuesString() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.STRING, false, 32));

		new Schema("foo", fields);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIncorrectDefaultValuesInt() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.INT, false, "foo"));

		new Schema("foo", fields);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testIncorrectDefaultValuesLong() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.LONG, false, 32));

		new Schema("foo", fields);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIncorrectDefaultValuesFloat() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.FLOAT, false, "xxx"));

		new Schema("foo", fields);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testIncorrectDefaultValuesDouble() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.DOUBLE, false, "xxx"));

		new Schema("foo", fields);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIncorrectDefaultValuesBoolean() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.BOOLEAN, false, "xxx"));

		new Schema("foo", fields);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testIncorrectDefaultValuesBytes() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.BYTES, false, "xxx"));

		new Schema("foo", fields);
	}
		
	@Test
	public void testCorrectDefaultValue() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo1", Type.BOOLEAN, false, true));		
		fields.add(Field.create("foo2", Type.BYTES, false, new byte[] { 1, 2 }));
		fields.add(Field.create("foo3", Type.INT, false, 12));
		fields.add(Field.create("foo4", Type.LONG, false, 12l));
		fields.add(Field.create("foo5", Type.FLOAT, false, 12.5f));
		fields.add(Field.create("foo6", Type.DOUBLE, false, 12.5d));
		fields.add(Field.create("foo7", Type.STRING, false, "foo"));
		
		new Schema("foo", fields);
		
		
	}
}
