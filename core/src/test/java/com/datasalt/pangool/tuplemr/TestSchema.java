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
}
