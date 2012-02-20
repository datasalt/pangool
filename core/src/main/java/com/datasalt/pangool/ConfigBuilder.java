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
	private String[] fieldsToPartition;
	
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
	
	private boolean fieldPresentInAllSources(String field){
		Class type = null;
		for (Schema source : sourceSchemas){
			if (!source.containsField(field)){
				return false;
			}
		}
		return true;
	}
	
	public void setGroupByFields(String... groupByFields) throws CoGrouperException {
		failIfEmpty(groupByFields,"GroupBy fields can't be null or empty");
		failIfEmpty(sourceSchemas,"No eschemas defined");
		failIfNotNull(this.groupByFields,"GroupBy fields already set : " + groupByFields);
		for (String field : groupByFields){
			if (!fieldPresentInAllSources(field)){
				throw new CoGrouperException("Can't group by field '" + field + "' . Not present in all sources");
			}
			if (!fieldSameTypeInAllSources(field)){
				throw new CoGrouperException("Can't group by field '" +field + "' since its type differs among sources");
			}
		}
		this.groupByFields = Arrays.asList(groupByFields);
	}
	
	public void setRollupFrom(String rollupFrom) throws CoGrouperException {
		failIfNull(rollupFrom,"Rollup can't be null");
		failIfNotNull(this.rollupFrom,"Rollup was already set");
		failIfEmpty(this.groupByFields,"GroupBy fields not set");
		if (!this.groupByFields.contains(rollupFrom)){
			throw new CoGrouperException("Rollup field must be present fields to group by '" + groupByFields + "'");
		}
		if (this.commonSortBy == null){
			//rollup needs explicit common orderby
			throw new CoGrouperException("No common order previously set");
		}
		
		this.rollupFrom = rollupFrom;
	}
	
	public void setCustomPartitionFields(String ... fields) throws CoGrouperException {
		failIfEmpty(fields,"Need to specify at leas tone field to partition by");
		//check if all fields are present in all sources and with the same type
		for (String field : fields){
			if (!fieldPresentInAllSources(field)){
				throw new CoGrouperException("Can't group by field '" + field + "' . Not present in all sources");
			}
			if (!fieldSameTypeInAllSources(field)){
				throw new CoGrouperException("Can't group by field '" +field + "' since its type differs among sources");
			}
		}
		this.fieldsToPartition = fields;
	}
	
//------------------------------------------------------------------------- //

	public void setOrderBy(SortBy ordering) throws CoGrouperException {
		failIfNull(ordering,"OrderBy can't be null");
		failIfEmpty(ordering.getElements(),"OrderBy can't be empty");
		failIfEmpty(sourceSchemas,"Need to specify source schemas");
		failIfEmpty(groupByFields,"Need to specify group by fields");
		if (sourceSchemas.size() == 1){
			if (ordering.getSourceOrderIndex() != null){
				throw new CoGrouperException("Not able to use source order when just one source specified");
			}
		}
		for (SortElement sortElement : ordering.getElements()){
			if (!fieldPresentInAllSources(sortElement.getName())){
				throw new CoGrouperException("Can't sort by field '" + sortElement.getName() + "' . Not present in all sources");
			}
			if (!fieldSameTypeInAllSources(sortElement.getName())){
				throw new CoGrouperException("Can't sort by field '" +sortElement.getName() + "' since its type differs among sources");
			}
		}
		//group by fields need to be a prefix of sort by fields
		for (String groupField : groupByFields){
			if (!ordering.containsBeforeSourceOrder(groupField)){
				throw new CoGrouperException("Group by field '" + groupField + "' is not present in common order by before source order");
			}
		}
		
		this.commonSortBy = ordering;
	}
		
	private Schema getSourceSchemaByName(String name){
		for (Schema s : sourceSchemas){
			if (s.getName().equals(name)){
				return s;
			}
		}
		return null;
	}
	
	public void setSecondaryOrderBy(String sourceName,SortBy ordering) throws CoGrouperException {
		failIfNull(sourceName,"Not able to set secondary sort for null source");
		if (!sourceAlreadyExists(sourceName)){
			throw new CoGrouperException("Unknown source '" + sourceName +"' in secondary SortBy");
		}
		failIfNull(ordering,"Not able to set null criteria for source '" + sourceName + "'");
		failIfEmpty(ordering.getElements(),"Can't set empty ordering");
		failIfNull(commonSortBy,"Not able to set secondary order with no previous common OrderBy");
		if (commonSortBy.getSourceOrderIndex() == null){
			throw new CoGrouperException("Need to specify source order in common SortBy when using secondary SortBy");
		}
		if (ordering.getSourceOrderIndex() != null){
			throw new CoGrouperException("Not allowed to set source order in secondary order");
		}
		Schema sourceSchema = getSourceSchemaByName(sourceName);
		for (SortElement e : ordering.getElements()){
			if (!sourceSchema.containsField(e.getName())){
				throw new CoGrouperException("Source '" + sourceName +"' doesn't contain field '" + e.getName());
			}
		}
		
		for (SortElement e : ordering.getElements()){
			if (commonSortBy.containsFieldName(e.getName())){
				throw new CoGrouperException("Common sort by already contains sorting for field '" + e.getName());
			}
		}
		this.secondarysOrderBy.put(sourceName, ordering);
	}
	

	public CoGrouperConfig buildConf() throws CoGrouperException {
		failIfEmpty(sourceSchemas," Need to declare at least one source schema");
		failIfEmpty(groupByFields," Need to declare group by fields");
		
		CoGrouperConfig conf =  new CoGrouperConfig();
		for (Schema schema : sourceSchemas){
			conf.addSource(schema);
		}
		
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
