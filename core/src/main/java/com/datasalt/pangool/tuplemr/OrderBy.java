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

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.tuplemr.Criteria.NullOrder;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * OrderBy is a convenience builder used by {@link TupleMRConfig} , similar to
 * {@link Criteria}. The main difference is that {@link OrderBy} is mutable
 * using the concatenation pattern and allows to specify schemaOrder.
 * <p/>
 * The OrderBy instances are converted to immutable Criteria objects by
 * {@link TupleMRConfig}.
 */
public class OrderBy {

  private Order schemaOrder;
  private Integer schemaOrderIndex;
  private List<SortElement> elements = new ArrayList<SortElement>();

  public OrderBy(List<SortElement> elements) {
    this.elements = elements;
  }

  public OrderBy() {
  }

  /**
   * Parse in the form "field1:asc, field2:desc,...,fieldn:asc|null_smallest"
   * <br/>
   * Examples:
   * <br/>
   * "f1,f2,f3"
   * <br/>
   * "f1:asc,f2:desc,f3"
   * <br/>
   * "f1,f2:asc|null_smallest,f3:desc|null_biggest"
   */
  public static OrderBy parse(String orderBy) {
    OrderBy toReturn = new OrderBy();
    String[] orderBys = orderBy.split(",");
    for (String order : orderBys) {
      order = order.trim();
      String[] fields = order.split(":");
      Order ord = Order.ASC;
      NullOrder nullOrd = NullOrder.NULL_SMALLEST;
      if (fields.length > 1) {
        String[] qualifiers = fields[1].split("\\|");
        for (String qualifier : qualifiers) {
          qualifier = qualifier.trim();
          boolean success = false;
          try {
            ord = Order.valueOf(qualifier.toUpperCase());
            success = true;
          } catch (IllegalArgumentException e) {
          }
          try {
            nullOrd = NullOrder.valueOf(qualifier.toUpperCase());
            success = true;
          } catch (IllegalArgumentException e) {
          }
          if (!success) {
            throw new PangoolRuntimeException("Unrecognised sort qualifier " + qualifier +
                " on sorting string: " + orderBy + ". Valid qualifiers are " + validQualifiers());
          }
        }
      }
      toReturn.add(fields[0].trim(), ord, nullOrd);
    }
    return toReturn;
  }

  private static String validQualifiers() {
    String ret = "";
    boolean first = true;
    for (Order o : Order.values()) {
      if (!first) {
        ret += ",";
      }
      ret += o;
      first = false;
    }
    ret += ",";
    first = true;
    for (NullOrder o : NullOrder.values()) {
      if (!first) {
        ret += ",";
      }
      ret += o;
      first = false;
    }
    return ret;
  }

  /**
   * Adds a new field to order by and its specified order.
   * <br/>
   * The default @{link NullOrder} used is for nullable fields is @{link NullOrder#NULL_SMALLEST}
   *
   * @param name  Field's name
   * @param order Field's order
   * @see Order
   */
  public OrderBy add(String name, Order order) {
    failIfFieldNamePresent(name);
    this.elements.add(new SortElement(name, order, NullOrder.NULL_SMALLEST));
    return this;
  }

  /**
   * Adds a new field to order by and its specified order.
   *
   * @param name      Field's name
   * @param order     Field's order
   * @param nullOrder Sorting of null values in nullable fields. {@link NullOrder#NULL_SMALLEST} if you want
   *                  nulls to be the smallest value or {@link NullOrder#NULL_BIGGEST} if you want
   *                  nulls to be the biggest. Ignored if fields are not nullable. Cannot be null.
   * @see Order
   * @see NullOrder
   */
  public OrderBy add(String name, Order order, NullOrder nullOrder) {
    failIfFieldNamePresent(name);
    this.elements.add(new SortElement(name, order, nullOrder));
    return this;
  }

  /**
   * Same as {@link OrderBy#add(String, Order)} but adding the possibility to
   * specify a custom comparator for that field.
   *
   * @param name       Field's name
   * @param order      Field's order
   * @param nullOrder  Sorting of null values in nullable fields. {@link NullOrder#NULL_SMALLEST} if you want
   *                   nulls to be the smallest value or {@link NullOrder#NULL_BIGGEST} if you want
   *                   nulls to be the biggest. Ignored if fields are not nullable. Cannot be null.
   * @param comparator Custom comparator instance
   * @see Order
   * @see NullOrder
   */
  public OrderBy add(String name, Order order, NullOrder nullOrder, RawComparator<?> comparator) {
    failIfFieldNamePresent(name);
    this.elements.add(new SortElement(name, order, nullOrder, comparator));
    return this;
  }


  /**
   * This method,unlike the traditional
   * {@link OrderBy#add(String, Order, NullOrder, RawComparator)} method, adds a symbolic
   * elements to order by.<p>
   * <p/>
   * This method only works in a multi-schema scenario, and it specifies that
   * tuples will be sorted by their schema,not by a field's name.<br>
   * <p/>
   * Example :<br>
   * b.addIntermediateSchema(schema1); b.addIntermediateSchema(schema2);<br>
   * b.setOrderBy(new OrderBy().add("user_id",Order.ASC).addSchemaOrder(Order.DESC));</br>
   * <p/>
   * In the case above, tuples will be first sorted by user_id and then if they
   * compare as equals then tuples from schema2 will sort before those from
   * schema1.
   * <p/>
   * This method must be called just once, and it's not allowed in
   * {@link TupleMRConfigBuilder#setSpecificOrderBy(String, OrderBy)}
   */
  public OrderBy addSchemaOrder(Order order) {
    if (this.schemaOrderIndex != null) {
      throw new IllegalStateException("The schema order is already set");
    }
    this.schemaOrder = order;
    this.schemaOrderIndex = getElements().size();
    return this;
  }

  /**
   * Returns a {@link SortElement} object for every field added to this builder.
   *
   * @see SortElement
   */
  public List<SortElement> getElements() {
    return elements;
  }

  /**
   * Gets the schemaOrder if set.
   */
  public Order getSchemaOrder() {
    return schemaOrder;
  }

  /**
   * Returns the position in the list where schemaOrder was added using
   * {@link OrderBy#addSchemaOrder(Order)}
   */
  public Integer getSchemaOrderIndex() {
    return schemaOrderIndex;
  }

  private void failIfFieldNamePresent(String name) {
    if (containsFieldName(name)) {
      throw new IllegalArgumentException("Sort element with field name '" + name
          + "' is already present");
    }
  }

  /**
   * True if field was added using {@link #add(String, Order)}
   */
  public boolean containsFieldName(String field) {
    for (SortElement e : elements) {
      if (e.getName().equals(field)) {
        return true;
      }
    }
    return false;
  }

  /**
   * True if field was added before calling {@link #addSchemaOrder(Order)}
   */
  public boolean containsBeforeSchemaOrder(String field) {
    if (schemaOrderIndex == null) {
      return containsFieldName(field);
    }
    for (int i = 0; i < schemaOrderIndex; i++) {
      if (elements.get(i).getName().equals(field)) {
        return true;
      }
    }
    return false;
  }

  public String toString() {
    ObjectMapper mapper = new ObjectMapper();

    try {
      return mapper.writeValueAsString(elements);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
