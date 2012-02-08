package com.datasalt.pangool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortCriteria.SortElement;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;


@SuppressWarnings("rawtypes")
public class CoGrouperConfigBuilder {

	private CoGrouperConfig config = new CoGrouperConfig();
	
	public static CoGrouperConfigBuilder newOne() {
		return new CoGrouperConfigBuilder();
	}
	
	public CoGrouperConfigBuilder setOrderBy(Ordering ordering) {
		config.setCommonOrder(ordering);
		return this;
	}
	
	public CoGrouperConfigBuilder addSecondaryOrderBy(String sourceName,Ordering ordering) {
		config.set(ordering);
		return this;
	}

	public CoGrouperConfigBuilder addSource(Schema schema) throws CoGrouperException {
		config.addSource(schema);
		return this;
	}
	
	public CoGrouperConfigBuilder setGroupByFields(String... groupByFields) {
		config.setGroupByFields(groupByFields);
		return this;
	}
	
	public CoGrouperConfigBuilder setRollupFrom(String rollupFrom) {
		config.setRollupFrom(rollupFrom);
		return this;
	}

	private void raiseExceptionIfNull(Object ob, String message) throws CoGrouperException {
		if(ob == null) {
			throw new CoGrouperException(message);
		}
	}

  private void raiseExceptionIfEmpty(Collection ob, String message) throws CoGrouperException {
		if(ob == null || ob.isEmpty()) {
			throw new CoGrouperException(message);
		}
	}

	/**
	 * build() called to finally calculate things like the common schema
	 * and perform sanity validation
	 */
	public CoGrouperConfig build() throws CoGrouperException {
		
		raiseExceptionIfEmpty(config.getSources().values(), "Need to set at least one source");
		raiseExceptionIfNull(config.getSorting(), "Need to set sorting");
		raiseExceptionIfNull(config.getSorting().getCommonSortCriteria(), "Need to set sorting criteria");
		raiseExceptionIfNull(config.getGroupByFields(), "Need to set fields to group by");
		return config;
	}
	

	
  public static CoGrouperConfig fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, CoGrouperException, NumberFormatException, InvalidFieldException {
		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
		configBuilder.config.fromJSON(json, mapper);
		return configBuilder.build();
	}
	
	public static CoGrouperConfig get(Configuration conf) throws CoGrouperException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		try {
			String serialized =conf.get(CoGrouperConfig.CONF_PANGOOL_CONF);
	    return (serialized == null || serialized.isEmpty()) ? null : fromJSON(serialized, jsonSerDe);
    } catch (JsonParseException e) {
    	fail(e);
    } catch(JsonMappingException e) {
    	fail(e);
    } catch(NumberFormatException e) {
    	fail(e);
    } catch(IOException e) {
    	fail(e);
    }
		return null;
	}
	
	private static void fail(Exception e) throws CoGrouperException {
		throw new CoGrouperException(e);
	}
}
