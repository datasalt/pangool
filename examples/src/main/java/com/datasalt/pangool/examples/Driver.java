package com.datasalt.pangool.examples;

import org.apache.hadoop.util.ProgramDriver;

import com.datasalt.pangool.examples.grep.Grep;
import com.datasalt.pangool.examples.simplesecondarysort.SecondarySort;
import com.datasalt.pangool.examples.wordcount.WordCount;


/**
 * <p>This is Hadoop's main entry point - here we'll add 
 * all the different programs that we want to execute with Hadoop.</p>
 * 
 */
public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();
		addClass("wordcount", WordCount.class, "Typical word count in Pangool");
		addClass("secondarysort", SecondarySort.class, "Typical secondary sort (two ints) in Pangool");
		addClass("grep", Grep.class, "Map-only job that performs Grep");
	}
	
	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
