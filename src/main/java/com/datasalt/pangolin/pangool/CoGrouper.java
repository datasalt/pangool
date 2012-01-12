package com.datasalt.pangolin.pangool;

/**
 * 
 * @author pere
 *
 */
public class CoGrouper {

	private PangoolConfig config;
	
	public CoGrouper() {
		config = new PangoolConfig();
	}
	
	public CoGrouper setSorting(Sorting sorting) {
		config.setSorting(sorting);
		return this;
	}
	
	public CoGrouper addSchema(String schemaId, Schema schema) {
		if(config.getSchemes().containsKey(schemaId)) {
			
		}
		
		if(schema == null) {
			
		}
		
		config.addSchema(schemaId, schema);
		return this;
	}
	
	public CoGrouper groupBy(String... fields) {
		config.setGroupByFields(fields);
		return this;
	}
	
	public CoGrouper setRollupFrom(String rollupFrom) {
		config.setRollupFrom(rollupFrom);
		return this;
	}
}