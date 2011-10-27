package com.datasalt.pangolin.commons.count;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

//import net.peerindex.pisae.commons.ObjectUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Class for counting, creating list of distinct elements and count distinct,
 * aggregating by several dimensions. <br>
 * For example: <code>
 * 	Counter c = new Counter(true);
 * 	c.in("Animals").in("Dog").in("Mastin").count("toby");
 * 	c.in("Animals").in("Dog").in("BullDog").count("toby");
 * 	c.in("Animals").in("Dog").in("BullDog").count("brian");
 * 
 *  System.out.println(ObjectUtils.toIndentedJSON(c.getCounts()));
 * </code>
 * 
 * would print:<br/>
 * 
 * <code>
 * {
 * "nextLevels" : {
 *   "Animals" : {
 *     "nextLevels" : {
 *       "Dog" : {
 *         "nextLevels" : {
 *           "Mastin" : {
 *             "nextLevels" : null,
 *             "distinctList" : [ "toby" ],
 *             "count" : 1
 *           },
 *           "BullDog" : {
 *             "nextLevels" : null,
 *             "distinctList" : [ "brian", "toby" ],
 *             "count" : 2
 *           }
 *         },
 *         "distinctList" : [ "brian", "toby" ],
 *         "count" : 3
 *       }
 *     },
 *     "distinctList" : [ "brian", "toby" ],
 *     "count" : 3
 *   }
 * },
 * "distinctList" : [ "brian", "toby" ],
 * "count" : 3
 * }
 * </code>
 * 
 * It supports whatever object as level, for example enums. <br/>
 * 
 * This class can compose distinct lists to count distinct elements. This class
 * is not thread safe.
 * 
 * @author ivan
 */
public class Counter {

  public static class CounterException extends Exception {
    private static final long serialVersionUID = 1L;

    public CounterException(String message) {
      super(message);
    }

  }
  
  
  private boolean countDistinct = true;

  /**
   * This method creates a counter that besides just counting numerically , it
   * keeps track of the distinct elements counted.
   * 
   * @return
   */
  public static Counter createWithDistinctElements() {
    return new Counter(true);
  }

  /**
   * This creates a counter that doesn't store elements, just numeric counters,
   * hence the methods to retrieve elements like {@link
   * getDistinctListAsStringList()}  {@link getDistinctList()} throw an exception.
   * 
   * @return
   */
  public static Counter numericCounter() {
    return new Counter(false);
  }

  public static class Count {

    private long count = 0;
    private Set<Comparable<?>> distinctList = null;
    private Map<Object, Count> nextLevels = null;
    private boolean countDistinct;

    protected Count(boolean countDistinct) {
      distinctList = Sets.newTreeSet();
      this.countDistinct = countDistinct;
    }

    /**
     * TODO Important! document this much better or refactor. This count is like a SELECT COUNT(*) , not a SELECT COUNT(DISTINCT) 
     * 
     * Explain that even with countDistinct=true the count refers to all the elements counted, 
     * even if they were repeated and don't appear in getDistinctList() 
     * 
     */
    public long getCount() {
      return count;
    }

    public Set<Comparable<?>> getDistinctList() throws CounterException {
      if (!countDistinct) {
        throw new CounterException(
            "unsupported method for counter with countDistinct=false.Please use : Counter.createWithDistinctElements() ");
      }
      return distinctList;
    }

    public List<String> getDistinctListAsStringList() throws CounterException { // Convenience
                                                                                // method
      if (!countDistinct) {
        throw new CounterException(
          "unsupported method for counter with non-distinct values. Please use : Counter.createWithDistinctElements()");
      }
      List<String> list = new ArrayList<String>(distinctList.size());
      for (Object obj : distinctList) {
        list.add(obj + "");
      }
      return list;
    }

    public Map<Object, Count> getNextLevels() {
      return nextLevels;
    }

    public Count get(Object o) {
      if (nextLevels != null) {
        Count co = nextLevels.get(o);
        if (co == null) {
          return new Count(countDistinct);
        } else {
          return co;
        }
      } else {
        return new Count(countDistinct);
      }
    }
  }

  private Count root;
  private List<Object> states = new ArrayList<Object>();

  public enum Type {
    COUNT, DISTINCT_LIST
  }

  private Counter() {
    root = new Count(countDistinct);
  }

  private Counter(boolean countDistinct) {
    this();
    this.countDistinct = countDistinct;
  }

  public Counter in(Comparable<?> o) {
    states.add(o);

    return this;
  }

  public void count(Comparable<?> o) {
    // Root level
    Count currentLevel = root;
    c(currentLevel, o);

    // Iterate over the given levels
    for (Object state : states) {

      if (currentLevel.getNextLevels() == null) {
        currentLevel.nextLevels = Maps.newHashMap();
      }

      Count nextLevel = currentLevel.getNextLevels().get(state);
      if (nextLevel == null) {
        nextLevel = new Count(countDistinct);
        currentLevel.getNextLevels().put(state, nextLevel);
      }

      c(nextLevel, o);
      currentLevel = nextLevel;
    }

    states.clear();
  }

  public Count getCounts() {
    return root;
  }

  private void c(Count currentLevel, Comparable<?> o) {
    currentLevel.count++;
    try {

      if (countDistinct) {
        Set<Comparable<?>> distinctSet;
        distinctSet = (Set<Comparable<?>>) currentLevel.getDistinctList();

        if (distinctSet == null) {
          distinctSet = Sets.newTreeSet();
          currentLevel.distinctList = distinctSet;
        }
        distinctSet.add(o);
      }
    } catch (Exception e) {
      // this is never reached }
    }

  }

  /**
   * @param args
   */
  public static void main(String args[]) {
    Counter c = new Counter(true);
    c.in("Animals").in("Dog").in("Mastin").count("toby");
    c.in("Animals").in("Dog").in("BullDog").count("toby");
    c.in("Animals").in("Dog").in("BullDog").count("brian");

    //System.out.println(ObjectUtils.toIndentedJSON(c.getCounts()));
  }
}
