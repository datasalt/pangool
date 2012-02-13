/**
 * Copyright [2011] [Datasalt Systems S.L.]
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

import java.io.Serializable;


import org.apache.hadoop.io.Text;

import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;

/**
 * This is the basic implementation of {@link ITuple}. It extends a HashMap<String, Object>
 */
@SuppressWarnings("serial")
public class Tuple implements ITuple,Serializable {

	private Object[] array;
	private Schema schema;

	public Tuple(Schema schema) {
		this.schema = schema;
		int size = schema.getFields().size();
		this.array = new Object[size];
//		if (initializeTexts){
//			initializeTexts(schema);
//		}
	}
	
//	private void initializeTexts(Schema schema){
//		for (int i=0 ; i < schema.getFields().size() ; i++){
//			if (schema.getField(i).getType() == String.class){
//				set(i,new Text());
//			}
//		}
//	}
	
//	public Tuple(Schema schema){
//		this(schema,false);
//	}
	
	@Override
	public Object get(int pos) {
		return array[pos];
	}

	@Override
	public void set(int pos, Object object) {
		array[pos] = object;
	}

	@Override
	public String toString(){
		return toString(this);
	}

	public static String toString(ITuple tuple) {
		Schema schema = tuple.getSchema();
		StringBuilder b = new StringBuilder();
		b.append("{");
		for (int i = 0 ; i < schema.getFields().size() ; i++){
			Field f = schema.getField(i);
			b.append("\"").append(f.name()).append("\"").append(":").append("\"").append(tuple.get(i)).append("\"").append(" , ");
		}
		String str = b.toString();
		//nasty
		return str.substring(0, str.length() - 2) + "}";
	}
	

	@Override
  public void clear() {
		if (array!= null){
			for (int i=0 ; i < array.length ; i++){
				array[i] = null;
			}
		}
  }

	@Override
  public Schema getSchema() {
	  return schema;
  }

		@Override
  public Object get(String field) {
	  return get(schema.getFieldPos(field));
  }

	@Override
  public void set(String field, Object object) {
	  set(schema.getFieldPos(field),object);
  }
}
