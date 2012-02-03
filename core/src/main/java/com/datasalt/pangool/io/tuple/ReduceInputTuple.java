package com.datasalt.pangool.io.tuple;

import java.util.HashMap;
import java.util.Map;

import com.datasalt.pangool.Schema;

public class ReduceInputTuple extends BaseTuple{

	static class Container {
	
		private Tuple commonTuple;

		private Map<Integer, Tuple> specificTuples;
		private int selectedSource;
		private Tuple currentSpecificTuple;

		public Container(Schema commonSchema, Map<Integer, Schema> specificSchemas) {
			commonTuple = new Tuple(commonSchema);
			specificTuples = new HashMap<Integer, Tuple>();
			for(Map.Entry<Integer, Schema> entry : specificSchemas.entrySet()) {
				int source = entry.getKey();
				Schema schema = entry.getValue();
				specificTuples.put(source, new Tuple(schema));
			}
		}

		void setCurrentSource(int sourceId) {
			this.selectedSource = sourceId;
			this.currentSpecificTuple = specificTuples.get(sourceId);
		}

		public int getCurrentSource() {
			return selectedSource;
		}

		public Tuple getSpecificTuple() {
			return currentSpecificTuple;
		}

		public Tuple getCommonTuple() {
			return commonTuple;
		}
	}

	private Container container;
	private Schema schema;
	private Object sourceId;
	private int sourceIdPos;
	
	public ReduceInputTuple(int sourceIdPos){
		this.sourceIdPos = sourceIdPos;
	}
	
	void setSchema(Schema schema){
		this.schema = schema;
	}
	
	void setSourceId(int sourceId){
		this.sourceId = sourceId;
	}
	
	@Override
  public Object get(int pos) {
	  if (pos < sourceIdPos){
	  	return container.commonTuple.get(pos);
	  } else if (pos > sourceIdPos){
	  	return container.getSpecificTuple().get(pos-sourceIdPos-1);
	  } else {
	  	return sourceId;
	  }
  }

	@Override
  public void set(int pos, Object object) {
		if (pos < sourceIdPos){
	  	container.commonTuple.set(pos,object);
	  } else if (pos > sourceIdPos){
	  	container.getSpecificTuple().set(pos-sourceIdPos-1,object);
	  } else {
	  	this.sourceId = object;
	  }
	  
  }

	@Override
  public Schema getSchema() {
	  return schema;
  }

	public void clear(){
		container.commonTuple.clear();
		container.getSpecificTuple().clear();
	}

	@Override
  public void set(String field, Object object) {
	  int pos = schema.indexByFieldName(field);
	  set(pos,object);
  }

	@Override
  public Object get(String field) {
		int pos = schema.indexByFieldName(field);
		return get(pos);
  }
	
}
