package com.datasalt.pangolin.mapred.counter;

import java.io.IOException;



/**
 * Inteface for emiting items to be counted. See {@link MapRedCounter.MapRedCounterMapper} 
 * 
 * @author ivan
 *
 */
public interface CountEmitInterface {
	
	/**
	 * Emits a new Item to be counted. After the execution
	 * of the counter will be present the following stats:<br/>
	 * [typeIdentifier, group, item] -> count <br/>
	 * [typeIdentifier, group] -> count, distinctItemsCount<br/>
	 * <br/>
	 * Also the list of distinct items per group will exist in a file.<br/>
	 * The typeIdentifier is there to be used for identifying
	 * the types of the group and the item. Because in the same file
	 * will be present counts for different groups and items that will 
	 * maybe be of different types, this number can be used to identify
	 * to which one it belongs. 
	 */
	void emit(int typeIdentifier, Object group, Object item) throws IOException, InterruptedException ;

}
