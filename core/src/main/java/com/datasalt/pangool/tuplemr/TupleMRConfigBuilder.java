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

import static com.datasalt.pangool.tuplemr.TupleMRException.failIfEmpty;
import static com.datasalt.pangool.tuplemr.TupleMRException.failIfNotNull;
import static com.datasalt.pangool.tuplemr.TupleMRException.failIfNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.mapred.Partitioner;

/**
 * 
 * ConfigBuilder is the responsible to create {@link TupleMRConfig} inmutable instances. 
 *
 */
public class TupleMRConfigBuilder {

	private List<Schema> schemas= new ArrayList<Schema>();
	private OrderBy commonSortBy;
	private Map<String,OrderBy> secondarysOrderBy=new HashMap<String,OrderBy>();
	private List<String> groupByFields;
	private String rollupFrom;
	private String[] fieldsToPartition;
	
	public TupleMRConfigBuilder(){}
	
	private Schema getSchemaByName(String name){
		for (Schema s : schemas){
			if (s.getName().equals(name)){
				return s;
			}
		}
		return null;
	}
	
	private boolean schemaAlreadyExists(String source){
		for (Schema schema : schemas){
			if (schema.getName().equals(source)){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Adds a Map-output schema. Tuples emitted by TupleMapper will use one of the schemas added by this method.
	 * Schemas added in consecutive calls to this method must be named differently.
	 */
	public void addIntermediateSchema(Schema schema) throws TupleMRException {
		if (schemaAlreadyExists(schema.getName())){
			throw new TupleMRException("There's a schema with that name '" + schema.getName() + "'");
		}
		schemas.add(schema);
	}
	
	private boolean fieldSameTypeInAllSources(String field){
		Type type = null;
		for (Schema source : schemas){
			Field f = source.getField(field);
			if (type == null){
				type = f.getType();
			} else if (type != f.getType()){
				return false;
			}
		}
		return true;
	}
	
	private boolean fieldPresentInAllSchemas(String field){
		for (Schema schema : schemas){
			if (!schema.containsField(field)){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Defines the fields used to group tuples by. Similar to the GROUP BY in SQL. 
	 * Tuples whose group-by fields are the same will be grouped and received in the same
	 * {@link TupleReducer#reduce} call.
	 * 
	 * When multiple schemas are set then the groupBy fields are used to perform co-grouping among tuples with different schemas.
	 * The groupBy fields specified in this method in a multi-source scenario must be present in every intermediate schema defined.
	 */
	public void setGroupByFields(String... groupByFields) throws TupleMRException {
		failIfEmpty(groupByFields,"GroupBy fields can't be null or empty");
		failIfEmpty(schemas,"No eschemas defined");
		failIfNotNull(this.groupByFields,"GroupBy fields already set : " + groupByFields);
		for (String field : groupByFields){
			if (!fieldPresentInAllSchemas(field)){
				throw new TupleMRException("Can't group by field '" + field + "' . Not present in all sources");
			}
			if (!fieldSameTypeInAllSources(field)){
				throw new TupleMRException("Can't group by field '" +field + "' since its type differs among sources");
			}
		}
		this.groupByFields = Arrays.asList(groupByFields);
	}
	
	
	//TODO document this
	/**
	 * 
	 * 
	 */
	public void setRollupFrom(String rollupFrom) throws TupleMRException {
		failIfNull(rollupFrom,"Rollup can't be null");
		failIfNotNull(this.rollupFrom,"Rollup was already set");
		failIfEmpty(this.groupByFields,"GroupBy fields not set");
		if (!this.groupByFields.contains(rollupFrom)){
			throw new TupleMRException("Rollup field must be present fields to group by '" + groupByFields + "'");
		}
		if (this.commonSortBy == null){
			//rollup needs explicit common orderby
			throw new TupleMRException("Rollup needs explicit order by. No common order previously set");
		}
		this.rollupFrom = rollupFrom;
	}
	
	
	//TODO explain how customPartitionFields works with rollup
	
	/**
	 * Sets the fields used to partition the tuples emmited by {@link TupleMapper}.
	 * The default implementation performs a partial hashing over the group-by fields. 
	 *   
	 * see {@link Partitioner}
	 */
	public void setCustomPartitionFields(String ... fields) throws TupleMRException {
		failIfEmpty(fields,"Need to specify at leas tone field to partition by");
		//check if all fields are present in all sources and with the same type
		for (String field : fields){
			if (!fieldPresentInAllSchemas(field)){
				throw new TupleMRException("Can't group by field '" + field + "' . Not present in all sources");
			}
			if (!fieldSameTypeInAllSources(field)){
				throw new TupleMRException("Can't group by field '" +field + "' since its type differs among sources");
			}
		}
		this.fieldsToPartition = fields;
	}
	
//------------------------------------------------------------------------- //

	/**
	 * Sets the criteria to sort the tuples by. In a multi-schema scenario all the fields 
	 * defined in the specified ordering must be present in every intermediate schema defined. 
	 * 
	 * see {@link OrderBy}
	 */
	public void setOrderBy(OrderBy ordering) throws TupleMRException {
		failIfNull(ordering,"OrderBy can't be null");
		failIfEmpty(ordering.getElements(),"OrderBy can't be empty");
		failIfEmpty(schemas,"Need to specify source schemas");
		failIfEmpty(groupByFields,"Need to specify group by fields");
		if (schemas.size() == 1){
			if (ordering.getSourceOrderIndex() != null){
				throw new TupleMRException("Not able to use source order when just one source specified");
			}
		}
		for (SortElement sortElement : ordering.getElements()){
			if (!fieldPresentInAllSchemas(sortElement.getName())){
				throw new TupleMRException("Can't sort by field '" + sortElement.getName() + "' . Not present in all sources");
			}
			if (!fieldSameTypeInAllSources(sortElement.getName())){
				throw new TupleMRException("Can't sort by field '" +sortElement.getName() + "' since its type differs among sources");
			}
		}
		//group by fields need to be a prefix of sort by fields
		for (String groupField : groupByFields){
			if (!ordering.containsBeforeSourceOrder(groupField)){
				throw new TupleMRException("Group by field '" + groupField + "' is not present in common order by before source order");
			}
		}
		
		this.commonSortBy = ordering;
	}
		
	
	
	//TODO improve doc. Not clear!!
	/**
	 * Sets how tuples from the specific schemaName will be sorted after being sorted by commonOrderBy and schemaOrder
	 * 
	 */
	public void setSpecificOrderBy(String schemaName,OrderBy ordering) throws TupleMRException {
		//TODO 
		failIfNull(schemaName,"Not able to set secondary sort for null source");
		if (!schemaAlreadyExists(schemaName)){
			throw new TupleMRException("Unknown source '" + schemaName +"' in secondary OrderBy");
		}
		failIfNull(ordering,"Not able to set null criteria for source '" + schemaName + "'");
		failIfEmpty(ordering.getElements(),"Can't set empty ordering");
		failIfNull(commonSortBy,"Not able to set secondary order with no previous common OrderBy");
		if (commonSortBy.getSourceOrderIndex() == null){
			throw new TupleMRException("Need to specify source order in common OrderBy when using secondary OrderBy");
		}
		if (ordering.getSourceOrderIndex() != null){
			throw new TupleMRException("Not allowed to set source order in secondary order");
		}
		Schema schema = getSchemaByName(schemaName);
		for (SortElement e : ordering.getElements()){
			if (!schema.containsField(e.getName())){
				throw new TupleMRException("Source '" + schemaName +"' doesn't contain field '" + e.getName());
			}
		}
		
		for (SortElement e : ordering.getElements()){
			if (commonSortBy.containsFieldName(e.getName())){
				throw new TupleMRException("Common sort by already contains sorting for field '" + e.getName());
			}
		}
		this.secondarysOrderBy.put(schemaName, ordering);
	}
	

	/**
	 * 
	 * Creates a brand new and immutable {@link TupleMRConfig} instance.
	 */
	public TupleMRConfig buildConf() throws TupleMRException {
		failIfEmpty(schemas," Need to declare at least one intermediate schema");
		failIfEmpty(groupByFields," Need to declare group by fields");
		
		TupleMRConfig conf =  new TupleMRConfig();
		conf.setIntermediateSchemas(schemas);
		conf.setGroupByFields(groupByFields);
		conf.setRollupFrom(rollupFrom);
		if (fieldsToPartition != null && fieldsToPartition.length != 0){
			conf.setCustomPartitionFields(Arrays.asList(fieldsToPartition));
		}
		
		Criteria convertedCommonOrder =convertCommonSortByToCriteria(commonSortBy);
		if (commonSortBy != null && commonSortBy.getSourceOrder() != null){
			conf.setSourceOrder(commonSortBy.getSourceOrder());
		} else {
			conf.setSourceOrder(Order.ASC);//by default source order is ASC
		}
		conf.setCommonCriteria(convertedCommonOrder);
		if (commonSortBy != null){
			Map<String,Criteria> convertedParticularOrderings = getSecondarySortBys(commonSortBy,schemas,secondarysOrderBy);
			for (Map.Entry<String,Criteria> entry : convertedParticularOrderings.entrySet()){
				conf.setSecondarySortBy(entry.getKey(), entry.getValue());
			}
		}
		return conf;
	}
	
	
	private Criteria convertCommonSortByToCriteria(OrderBy orderBy){
		if (orderBy == null){
			//then the common sortBy is by default the group fields in ASC order
			List<SortElement> elements = new ArrayList<SortElement>();
			for (String groupField : groupByFields){
				elements.add(new SortElement(groupField,Order.ASC));
			}
			return new Criteria(elements);
		} else if (orderBy.getSourceOrderIndex() == null || orderBy.getSourceOrderIndex() == orderBy.getElements().size()){
			return new Criteria(orderBy.getElements());
		} else {
			List<SortElement> sortElements = orderBy.getElements().subList(0,orderBy.getSourceOrderIndex());
			return new Criteria(sortElements);
		}
	}
	
	private static Map<String,Criteria> getSecondarySortBys(OrderBy commonSortBy,List<Schema> sourceSchemas,Map<String,OrderBy> secondarys){
		if (secondarys == null){
			return null;
		} else if (commonSortBy == null){
			throw new IllegalArgumentException("Common sort by must not be null if secondary sort by is set");
		}	else if (commonSortBy.getSourceOrderIndex() == null || commonSortBy.getSourceOrderIndex() == commonSortBy.getElements().size()){
			Map<String,Criteria> result = new HashMap<String,Criteria>();
			for (Map.Entry<String,OrderBy> entry : secondarys.entrySet()){
				result.put(entry.getKey(),new Criteria(entry.getValue().getElements()));
			}
			return result;
		} else {
			List<SortElement> toPrepend = commonSortBy.getElements().subList(commonSortBy.getSourceOrderIndex(),commonSortBy.getElements().size());
			Map<String,Criteria> result = new HashMap<String,Criteria>();
			for (Schema sourceSchema : sourceSchemas){
				String source = sourceSchema.getName();
				List<SortElement> newList = new ArrayList<SortElement>();
				newList.addAll(toPrepend);
				OrderBy criteria = secondarys.get(source);
				if (criteria != null){
					newList.addAll(criteria.getElements());
				}
				result.put(source,new Criteria(newList));
			}
			return result;
		}
	}
	
	/**
   * Initializes the custom comparator instances inside the given config criterias, 
   * calling the {@link Configurable#setConf(Configuration)} method. 
   */
  public static void initializeComparators(Configuration conf, TupleMRConfig groupConfig) {
  	TupleMRConfigBuilder.initializeComparators(conf, groupConfig.getCommonCriteria());
  	for(Criteria criteria : groupConfig.getSecondarySortBys()) {
  		if (criteria != null) {
  			TupleMRConfigBuilder.initializeComparators(conf, criteria);
  		}
  	}
  }
  
	private static void initializeComparators(Configuration conf, Criteria criteria) {
  	for (SortElement element : criteria.getElements()) {
  		RawComparator<?> comparator = element.getCustomComparator();
  		if (comparator != null && comparator instanceof Configurable) { 				
  			((Configurable) comparator).setConf(conf);
  		}
  	}
  }
}
