package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Criteria.SortElement;

public class ConfigBuilder {

	private List<Schema> sourceSchemas= new ArrayList<Schema>();
	private SortBy commonSortBy;
	private Map<String,SortBy> secondarysOrderBy=new HashMap<String,SortBy>();
	private String[]groupByFields;
	private String rollupFrom;
	
	public ConfigBuilder(){
		
	}
	
	public void addSourceSchema(Schema schema) throws CoGrouperException {
		sourceSchemas.add(schema);
	}
	
	public void setGroupByFields(String... groupByFields) {
			this.groupByFields = groupByFields;
		
	}
	
	public void setRollupFrom(String rollupFrom) {
		if (this.rollupFrom != null){
			throw new UnsupportedOperationException("Rollup was already set : " + rollupFrom);
		}
		this.rollupFrom = rollupFrom;
	}
	
//------------------------------------------------------------------------- //

	public void setOrderBy(SortBy ordering) {
		this.commonSortBy = ordering;
	}
	
	public void setSecondaryOrderBy(String sourceName,SortBy ordering){
		this.secondarysOrderBy.put(sourceName, ordering);
	}
	
	public CoGrouperConfig buildConf(){
		CoGrouperConfig conf =  new CoGrouperConfig();
		for (Schema schema : sourceSchemas){
			conf.addSource(schema);
		}
		
		conf.setGroupByFields(groupByFields);
		conf.setRollupFrom(rollupFrom);
		Criteria convertedCommonOrder =convertCommonSortByToCriteria(commonSortBy);
		if (commonSortBy.getSourceOrder() != null){
			conf.setSourceOrder(commonSortBy.getSourceOrder());
		} else {
			conf.setSourceOrder(Order.ASC);
		}
		
		conf.setCommonSortBy(convertedCommonOrder);
		
		if (commonSortBy != null){
			Map<String,Criteria> convertedParticularOrderings = getSecondarySortBys(commonSortBy, secondarysOrderBy);
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
	
	private static Map<String,Criteria> getSecondarySortBys(SortBy commonSortBy,Map<String,SortBy> secondarys){
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
			for (Map.Entry<String,SortBy> entry : secondarys.entrySet()){
				SortBy criteria = entry.getValue();
				List<SortElement> newList = new ArrayList<SortElement>();
				newList.addAll(toPrepend);
				newList.addAll(criteria.getElements());
				result.put(entry.getKey(),new Criteria(newList));
			}
			return result;
		}
	}
	
}
