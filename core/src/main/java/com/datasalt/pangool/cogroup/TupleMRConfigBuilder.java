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
package com.datasalt.pangool.cogroup;

import static com.datasalt.pangool.cogroup.TupleMRException.failIfEmpty;
import static com.datasalt.pangool.cogroup.TupleMRException.failIfNotNull;
import static com.datasalt.pangool.cogroup.TupleMRException.failIfNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

import com.datasalt.pangool.cogroup.sorting.Criteria;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.cogroup.sorting.Criteria.SortElement;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;

/**
 * 
 * ConfigBuilder is the responsible to create {@link TupleMRConfig} inmutable instances. 
 *
 */
public class TupleMRConfigBuilder {

	private List<Schema> intermediateSchemas= new ArrayList<Schema>();
	private SortBy commonSortBy;
	private Map<String,SortBy> secondarysOrderBy=new HashMap<String,SortBy>();
	private List<String> groupByFields;
	private String rollupFrom;
	private String[] fieldsToPartition;
	
	public TupleMRConfigBuilder(){
		
	}
	
	private boolean sourceAlreadyExists(String source){
		for (Schema sourceSchema : intermediateSchemas){
			if (sourceSchema.getName().equals(source)){
				return true;
			}
		}
		return false;
	}
	
	public void addIntermediateSchema(Schema schema) throws TupleMRException {
		if (sourceAlreadyExists(schema.getName())){
			throw new TupleMRException("There's a schema with that name '" + schema.getName() + "'");
		}
		intermediateSchemas.add(schema);
	}
	
	private boolean fieldSameTypeInAllSources(String field){
		Class<?> type = null;
		for (Schema source : intermediateSchemas){
			Field f = source.getField(field);
			if (type == null){
				type = f.getType();
			} else if (type != f.getType()){
				return false;
			}
		}
		return true;
	}
	
	private boolean fieldPresentInAllSources(String field){
		for (Schema source : intermediateSchemas){
			if (!source.containsField(field)){
				return false;
			}
		}
		return true;
	}
	
	public void setGroupByFields(String... groupByFields) throws TupleMRException {
		failIfEmpty(groupByFields,"GroupBy fields can't be null or empty");
		failIfEmpty(intermediateSchemas,"No eschemas defined");
		failIfNotNull(this.groupByFields,"GroupBy fields already set : " + groupByFields);
		for (String field : groupByFields){
			if (!fieldPresentInAllSources(field)){
				throw new TupleMRException("Can't group by field '" + field + "' . Not present in all sources");
			}
			if (!fieldSameTypeInAllSources(field)){
				throw new TupleMRException("Can't group by field '" +field + "' since its type differs among sources");
			}
		}
		this.groupByFields = Arrays.asList(groupByFields);
	}
	
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
	
	public void setCustomPartitionFields(String ... fields) throws TupleMRException {
		failIfEmpty(fields,"Need to specify at leas tone field to partition by");
		//check if all fields are present in all sources and with the same type
		for (String field : fields){
			if (!fieldPresentInAllSources(field)){
				throw new TupleMRException("Can't group by field '" + field + "' . Not present in all sources");
			}
			if (!fieldSameTypeInAllSources(field)){
				throw new TupleMRException("Can't group by field '" +field + "' since its type differs among sources");
			}
		}
		this.fieldsToPartition = fields;
	}
	
//------------------------------------------------------------------------- //

	public void setOrderBy(SortBy ordering) throws TupleMRException {
		failIfNull(ordering,"OrderBy can't be null");
		failIfEmpty(ordering.getElements(),"OrderBy can't be empty");
		failIfEmpty(intermediateSchemas,"Need to specify source schemas");
		failIfEmpty(groupByFields,"Need to specify group by fields");
		if (intermediateSchemas.size() == 1){
			if (ordering.getSourceOrderIndex() != null){
				throw new TupleMRException("Not able to use source order when just one source specified");
			}
		}
		for (SortElement sortElement : ordering.getElements()){
			if (!fieldPresentInAllSources(sortElement.getName())){
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
		
	private Schema getIntermediateSchemaByName(String name){
		for (Schema s : intermediateSchemas){
			if (s.getName().equals(name)){
				return s;
			}
		}
		return null;
	}
	
	public void setSecondaryOrderBy(String sourceName,SortBy ordering) throws TupleMRException {
		failIfNull(sourceName,"Not able to set secondary sort for null source");
		if (!sourceAlreadyExists(sourceName)){
			throw new TupleMRException("Unknown source '" + sourceName +"' in secondary SortBy");
		}
		failIfNull(ordering,"Not able to set null criteria for source '" + sourceName + "'");
		failIfEmpty(ordering.getElements(),"Can't set empty ordering");
		failIfNull(commonSortBy,"Not able to set secondary order with no previous common OrderBy");
		if (commonSortBy.getSourceOrderIndex() == null){
			throw new TupleMRException("Need to specify source order in common SortBy when using secondary SortBy");
		}
		if (ordering.getSourceOrderIndex() != null){
			throw new TupleMRException("Not allowed to set source order in secondary order");
		}
		Schema sourceSchema = getIntermediateSchemaByName(sourceName);
		for (SortElement e : ordering.getElements()){
			if (!sourceSchema.containsField(e.getName())){
				throw new TupleMRException("Source '" + sourceName +"' doesn't contain field '" + e.getName());
			}
		}
		
		for (SortElement e : ordering.getElements()){
			if (commonSortBy.containsFieldName(e.getName())){
				throw new TupleMRException("Common sort by already contains sorting for field '" + e.getName());
			}
		}
		this.secondarysOrderBy.put(sourceName, ordering);
	}
	

	public TupleMRConfig buildConf() throws TupleMRException {
		failIfEmpty(intermediateSchemas," Need to declare at least one intermediate schema");
		failIfEmpty(groupByFields," Need to declare group by fields");
		
		TupleMRConfig conf =  new TupleMRConfig();
		conf.setIntermediateSchemas(intermediateSchemas);
		
		
		conf.setGroupByFields(groupByFields);
		conf.setRollupFrom(rollupFrom);
		if (fieldsToPartition != null && fieldsToPartition.length != 0){
			conf.setCustomPartitionFields(Arrays.asList(fieldsToPartition));
		}
		
		Criteria convertedCommonOrder =convertCommonSortByToCriteria(commonSortBy);
		if (commonSortBy != null && commonSortBy.getSourceOrder() != null){
			conf.setSourceOrder(commonSortBy.getSourceOrder());
		} else {
			conf.setSourceOrder(Order.ASC);
		}
		
		conf.setCommonCriteria(convertedCommonOrder);
		
		
		if (commonSortBy != null){
			Map<String,Criteria> convertedParticularOrderings = getSecondarySortBys(commonSortBy,intermediateSchemas,secondarysOrderBy);
			for (Map.Entry<String,Criteria> entry : convertedParticularOrderings.entrySet()){
				conf.setSecondarySortBy(entry.getKey(), entry.getValue());
			}
		}
		return conf;
	}
	
	private Criteria convertCommonSortByToCriteria(SortBy sortBy){
		if (sortBy == null){
			//then the common sortBy is by default the group fields in ASC order
			List<SortElement> elements = new ArrayList<SortElement>();
			for (String groupField : groupByFields){
				elements.add(new SortElement(groupField,Order.ASC));
			}
			return new Criteria(elements);
		} else if (sortBy.getSourceOrderIndex() == null || sortBy.getSourceOrderIndex() == sortBy.getElements().size()){
			return new Criteria(sortBy.getElements());
		} else {
			List<SortElement> sortElements = sortBy.getElements().subList(0,sortBy.getSourceOrderIndex());
			return new Criteria(sortElements);
		}
	}
	
	private static Map<String,Criteria> getSecondarySortBys(SortBy commonSortBy,List<Schema> sourceSchemas,Map<String,SortBy> secondarys){
		if (secondarys == null){
			return null;
		} else if (commonSortBy == null){
			throw new IllegalArgumentException("Common sort by must not be null if secondary sort by is set");
		}	else if (commonSortBy.getSourceOrderIndex() == null || commonSortBy.getSourceOrderIndex() == commonSortBy.getElements().size()){
			Map<String,Criteria> result = new HashMap<String,Criteria>();
			for (Map.Entry<String,SortBy> entry : secondarys.entrySet()){
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
				SortBy criteria = secondarys.get(source);
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
