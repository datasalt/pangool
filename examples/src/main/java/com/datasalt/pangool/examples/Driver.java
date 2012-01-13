package com.datasalt.pangool.examples;

import org.apache.hadoop.util.ProgramDriver;

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
	}
	
	public static void main(String[] args) throws Throwable {
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
