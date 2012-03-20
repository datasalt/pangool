package com.datasalt.pangool.flow.ops;

import java.io.Serializable;
import java.util.regex.Pattern;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;

@SuppressWarnings("serial")
public class TupleParser implements Serializable {

	private Tuple tuple;
	private String splitterRegex;
	transient private Schema schema;
	transient private Pattern splitterPattern;
	
	public TupleParser(Schema schema, String splitterRegex) {
		tuple = new Tuple(schema);
		this.schema = schema;
		this.splitterRegex = splitterRegex;
	}
	
	public Schema getSchema() {
		return schema;
	}
	
	public Tuple parse(String input) {
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
		return tuple;
	}
}
