package com.datasalt.pangool.flow.ops;

import java.io.IOException;
import java.util.Map;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;

/**
 * A special operation that implements a 1-N Tuple join. It copies all the fields from the left tuple
 * and them all the fields from the right tuple. If the 1 side of the relationship is not present, it applies
 * the default values defined in the Map<String, Object> that is given as a parameter. The joint schema has to be given
 * as well. 
 */
@SuppressWarnings("serial")
public class OneToNJoinOp extends TupleOp<Iterable<ITuple>> {

	private Schema leftSchema;
	private Schema rightSchema;
	private Map<String, Object> defaultValues; // this is not a Tuple because it might be a part of a schema

	public OneToNJoinOp(Schema jointSchema, Schema leftSchema, Schema rightSchema, Map<String, Object> defaultValues) {
		super(jointSchema);
		this.leftSchema = leftSchema;
		this.rightSchema = rightSchema;
		this.defaultValues = defaultValues;
	}

	@Override
	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

		int count = 0;
		ITuple oneSchemaTuple = null;
		for(ITuple tuple : tuples) {
			if(count == 0) {
				if(tuple.getSchema().getName().equals(leftSchema.getName())) {
					oneSchemaTuple = tuple;
				}
			} else { // apply join operation
				if(tuple.getSchema().getName().equals(leftSchema.getName())) {
					throw new IOException("Expected to receive one tuple from left schema [" + leftSchema.getName() + "] but received more than one!");
				}
				if(oneSchemaTuple != null) {
					for(Field field: leftSchema.getFields()) {
						this.tuple.set(field.getName(), oneSchemaTuple.get(field.getName()));
					}
				} else {
					for(Map.Entry<String, Object> mapEntry: defaultValues.entrySet()) {
						this.tuple.set(mapEntry.getKey(), mapEntry.getValue());
					}
				}
				for(Field field: rightSchema.getFields()) {
					this.tuple.set(field.getName(), tuple.get(field.getName()));
				}
				callback.onReturn(this.tuple);
			}
			count++;
		}
	}
}
