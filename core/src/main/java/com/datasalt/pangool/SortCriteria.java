package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.RawComparator;

import com.datasalt.pangool.Schema.Field;

/**
 * Encapsulates one sorting configuration composed of {@link SortElement}s.
 * 
 * @author pere
 *
 */
@SuppressWarnings("rawtypes")
public class SortCriteria {

	boolean hasSourceIdField;
	
	public static class SortElement {

		private String fieldName;
		private SortOrder sortOrder;
    private Class<? extends RawComparator> comparator;

    public SortElement(String name, SortOrder sortOrder, Class<? extends RawComparator> comparator) {
			this.fieldName = name;
			this.sortOrder = sortOrder;
			this.comparator = comparator;
		}

		public String getFieldName() {
			return fieldName;
		}

		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}

		public void setSortOrder(SortOrder sortOrder) {
			this.sortOrder = sortOrder;
		}

		public void setComparator(Class<? extends RawComparator<?>> comparator) {
			this.comparator = comparator;
		}

		public SortOrder getSortOrder() {
			return sortOrder;
		}

    public Class<? extends RawComparator> getComparator() {
			return comparator;
		}
    
    public String toString() {
    	return fieldName + " " + sortOrder;
    }
	}

	public static enum SortOrder {
		ASC("asc"), DESC("desc");

		private String abr;

		private SortOrder(String abr) {
			this.abr = abr;
		}

		public String getAbreviation() {
			return abr;
		}
	}

	SortCriteria(SortElement[] sortElements) {
		this.sortElements = sortElements;
		this.hasSourceIdField = false;
		for(SortElement sortElement : sortElements) {
			if(sortElement.getFieldName().equals(Field.SOURCE_ID_FIELD_NAME)) {
				this.hasSourceIdField = true;
			}
		}
	}

	boolean hasSourceIdField() {
  	return hasSourceIdField;
  }

	private SortElement[] sortElements;

	public SortElement[] getSortElements() {
		return sortElements;
	}

	@SuppressWarnings("unchecked")
  static SortCriteria parse(String sortCriteria) throws CoGrouperException {
		List<SortElement> sortElements = new ArrayList<SortElement>();
		List<String> fields = new ArrayList<String>();
		String[] tokens = sortCriteria.split(",");
		for(String token : tokens) {

			String[] nameSort = token.trim().split("\\s+");
			if(nameSort.length < 2 || nameSort.length > 4) {
				throw new CoGrouperException("Invalid sortCriteria format : " + sortCriteria);
			}
			String name = nameSort[0];
			if(fields.contains(name)) {
				throw new CoGrouperException("Invalid sortCriteria .Repeated field " + name);
			}
			fields.add(name);
			int offset = 0;
			Class<? extends RawComparator<?>> comparator = null;
			try {
				if("using".equals(nameSort[1].toLowerCase())) {
					comparator = (Class<? extends RawComparator<?>>) Class.forName(nameSort[2]);
					offset = 2;
				}
			} catch(ClassNotFoundException e) {
				throw new CoGrouperException("Class not found : " + nameSort[2], e);
			}

			SortOrder sortOrder;
			if("ASC".equals(nameSort[1 + offset].toUpperCase())) {
				sortOrder = SortOrder.ASC;
			} else if("DESC".equals(nameSort[1 + offset].toUpperCase())) {
				sortOrder = SortOrder.DESC;
			} else {
				throw new CoGrouperException("Invalid SortCriteria " + nameSort[1] + " in " + sortCriteria);
			}

			SortElement sortElement = new SortElement(name, sortOrder, comparator);
			sortElements.add(sortElement);
		}

		SortElement[] array = new SortElement[sortElements.size()];
		sortElements.toArray(array);
		return new SortCriteria(array);
	}

	@Override
	public String toString() {

		StringBuilder b = new StringBuilder();

		for(int i = 0; i < sortElements.length; i++) {
			if(i != 0) {
				b.append(",");
			}
			SortElement sortElement = sortElements[i];
			b.append(sortElement.getFieldName());
			Class<?> comparator = sortElement.getComparator();
			if(comparator != null) {
				b.append(" using ").append(comparator.getName());
			}
			b.append(" ").append(sortElement.getSortOrder().getAbreviation());
		}
		return b.toString();
	}
}