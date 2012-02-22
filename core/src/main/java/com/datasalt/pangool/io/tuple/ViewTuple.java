package com.datasalt.pangool.io.tuple;


/**
 * A {@link ITuple} with a contained one, but that creates a
 * read only view over it and filtering only for some fields. 
 * Useful for creating views over {@link ITuple} for some
 * particular fields.
 */
public class ViewTuple implements ITuple {

	private ITuple contained;
	private Schema schema;
	private int[] indexTranslation;
	

	public ViewTuple(Schema schema) { 
		this.schema = schema;
	}
	
	public ViewTuple(Schema schema,int[] indexTranslation){
		this.schema = schema;
		this.indexTranslation = indexTranslation;
	}
	
	public void setContained(ITuple delegatedTuple,int[] indexTranslation) {
		contained = delegatedTuple;
		this.indexTranslation = indexTranslation;
	}
	
	public void setContained(ITuple contained){
		this.contained = contained;
	}
	
	private void fail() {
		throw new RuntimeException("Trying to modify a read only tuple. This is not allowed");
	}

	@Override
  public Object get(int pos) {
		if (pos >= schema.getFields().size()){
			throw new IllegalArgumentException("Field '"+ pos + "' out of bounds in schema:" + schema);
		}
		if (indexTranslation != null){
			return contained.get(indexTranslation[pos]);
		} else {
			String fieldName =schema.getField(pos).getName();
			return contained.get(fieldName);
		}
  }

	@Override
  public void set(int pos, Object object) {
		fail();
	}

	@Override
  public void clear() {
	  fail();
  }

	@Override
  public Schema getSchema() {
	  return schema;
  }
	
	@Override
  public Object get(String field) {
		Integer pos = schema.getFieldPos(field);
		if (pos == null){
			throw new IllegalArgumentException("Not known field '" + field + "' in schema:" + schema);
		}
	  return get(pos);
  }

	@Override
  public void set(String field, Object object) {
	  fail();
  }
	
	public String toString(){
		return Tuple.toString(this);
	}
	
}
