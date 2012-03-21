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
package com.datasalt.pangool.flow.ops;

import java.util.regex.Pattern;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;

/**
 * Operation that converts a text line into a Tuple of primitive types. 
 */
@SuppressWarnings("serial")
public class TupleParser extends TupleOp<String> {

	private String splitterRegex;
	transient private Pattern splitterPattern;
	
	public TupleParser(Schema schema, String splitterRegex) {
		super(schema);
		this.splitterRegex = splitterRegex;
	}
	
	public void process(String input, ReturnCallback<ITuple> callback) {
		if(splitterPattern == null) {
			splitterPattern = Pattern.compile(splitterRegex);
		}
		String[] fields = splitterPattern.split(input);
		Schema schema = tuple.getSchema();
		int index = -1;
		for(Field field: schema.getFields()) {
			Type type = field.getType();
			index++;
			switch(type) {
			case DOUBLE:
				tuple.set(field.getName(), Double.parseDouble(fields[index]));
				break;
			case BOOLEAN:
				tuple.set(field.getName(), Boolean.parseBoolean(fields[index]));
				break;
			case FLOAT:
				tuple.set(field.getName(), Float.parseFloat(fields[index]));
				break;
			case INT:
				tuple.set(field.getName(), Integer.parseInt(fields[index]));
				break;
			case LONG:
				tuple.set(field.getName(), Long.parseLong(fields[index]));
				break;
			case STRING:
				tuple.set(field.getName(), fields[index]);
				break;
			default:
				throw new RuntimeException("Not implemented, type: " + type + " in " + this.getClass().getName());
			}
		}
		callback.onReturn(tuple);
	}
}
