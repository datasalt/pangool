package com.datasalt.pangool;

import static com.datasalt.pangool.CoGrouperException.failIfEmpty;
import static com.datasalt.pangool.CoGrouperException.failIfNotNull;
import static com.datasalt.pangool.CoGrouperException.failIfNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Criteria.SortElement;
import com.datasalt.pangool.Schema.Field;

public class ConfigBuilder {

	private List<Schema> sourceSchemas= new ArrayList<Schema>();
	private SortBy commonSortBy;
	private Map<String,SortBy> secondarysOrderBy=new HashMap<String,SortBy>();
	private List<String> groupByFields;
	private String rollupFrom;
	
	public ConfigBuilder(){
		
	}
	
	private boolean sourceAlreadyExists(String source){
		for (Schema sourceSchema : sourceSchemas){
			if (sourceSchema.getName().equals(source)){
				return true;
			}
		}
		return false;
	}
	
	public void addSourceSchema(Schema schema) throws CoGrouperException {
		if (sourceAlreadyExists(schema.getName())){
			throw new CoGrouperException("There's a schema with that name '" + schema.getName() + "'");
		}
		sourceSchemas.add(schema);
	}
	
	private boolean fieldSameTypeInAllSources(String field){
		Class type = null;
		for (Schema source : sourceSchemas){
			Field f = source.getField(field);
			if (type == null){
				type = f.getType();
			} else if (type != f.getType()){
				return false;
			}
		}
		return true;
	}
	
	public void setGroupByFields(String... groupByFields) throws CoGrouperException {
		failIfEmpty(groupByFields,"GroupBy fields can't be null or empty");
		failIfEmpty(sourceSchemas,"No eschemas defined");
		failIfNotNull(this.groupByFields,"GroupBy fields already set : " + groupByFields);
		// check that all sources contain the fields
		for (String field : groupByFields){
			for(Schema source : this.sourceSchemas){
				if (!source.containsField(field)){
					throw new CoGrouperException("Can't group by field '" + field + "' . Source ' " + source + "' doesn't contain it");
				}
				if (!fieldSameTypeInAllSources(field)){
					throw new CoGrouperException("Can't group by field '" +field + "' since its type differs among sources");
				}
				
			}
		}
		this.groupByFields = Arrays.asList(groupByFields);
	}
	
	public void setRollupFrom(String rollupFrom) throws CoGrouperException {
		failIfNull(rollupFrom,"Rollup can't be null");
		failIfNotNull(this.rollupFrom,"Rollup was already set");
		failIfEmpty(this.groupByFields,"GroupBy fields not set");
		if (!this.groupByFields.contains(rollupFrom)){
			throw new CoGrouperException("Rollup field must be in fields to group by '" + groupByFields + "'");
		}
		
		
		this.rollupFrom = rollupFrom;
	}
	
//------------------------------------------------------------------------- //

	public void setOrderBy(SortBy ordering) throws CoGrouperException {
		failIfNull(ordering,"OrderBy can't be null");
		failIfEmpty(sourceSchemas,"Need to specify source schemas");
		failIfEmpty(groupByFields,"Need to specify group by fields");
		if (sourceSchemas.size() == 1){
			if (ordering.getSourceOrderIndex() != null){
				throw new CoGrouperException("Not able to use source order when just one source specified");
			}
		}
		//TODO check that common sort by contains fields that are present in all sources (same name and same type)
		
		//TODO common sort by needs to be a prefix of group by
		this.commonSortBy = ordering;
	}
	
	public void setSecondaryOrderBy(String sourceName,SortBy ordering) throws CoGrouperException {
		failIfNull(sourceName,"Not able to set secondary sort for null source");
		if (!sourceAlreadyExists(sourceName)){
			throw new CoGrouperException("Unknown source '" + sourceName +"' in secondary SortBy");
		}
		failIfNull(ordering,"Not able to set null criteria for source '" + sourceName + "'");
		failIfNull(commonSortBy,"Not able to set secondary order with no previous OrderBy");
		if (commonSortBy.getSourceOrderIndex() == null){
			throw new CoGrouperException("Need to specify source order in common SortBy when using secondary SortBy");
		}
		
		this.secondarysOrderBy.put(sourceName, ordering);
	}
	
//	protected void checkSortBys() throws CoGrouperException {
//		//Common sort by
//		
//		
//		
//		 
//		
//		//TODO 
//	}
//	
	
	public CoGrouperConfig buildConf() throws CoGrouperException {
		failIfEmpty(sourceSchemas," Need to declare at least one source schema");
		failIfEmpty(groupByFields," Need to declare group by fields");
//		checkSortBys();
		
		CoGrouperConfig conf =  new CoGrouperConfig();
		for (Schema schema : sourceSchemas){
			conf.addSource(schema);
		}
		
		conf.setGroupByFields(groupByFields);
		conf.setRollupFrom(rollupFrom);
		Criteria convertedCommonOrder =convertCommonSortByToCriteria(commonSortBy);
		if (commonSortBy != null && commonSortBy.getSourceOrder() != null){
			conf.setSourceOrder(commonSortBy.getSourceOrder());
		} else {
			conf.setSourceOrder(Order.ASC);
		}
		
		conf.setCommonCriteria(convertedCommonOrder);
		
		
		if (commonSortBy != null){
			Map<String,Criteria> convertedParticularOrderings = getSecondarySortBys(commonSortBy,sourceSchemas,secondarysOrderBy);
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
	
}
