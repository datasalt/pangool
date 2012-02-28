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
package com.datasalt.pangool.examples;

import org.apache.hadoop.util.ProgramDriver;

import com.datasalt.pangool.examples.movingaverage.MovingAverage;
import com.datasalt.pangool.examples.simplesecondarysort.SecondarySort;
import com.datasalt.pangool.examples.useractivitynormalizer.UserActivityNormalizer;

/**
 * This is Hadoop's main entry point - here we'll add 
 * all the different programs that we want to execute with Hadoop.
 */
public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();
		addClass("wordcount", PangoolWordCount.class, "Typical word count in Pangool");
		addClass("secondarysort", SecondarySort.class, "Typical secondary sort (two ints) in Pangool");
		addClass("grep", Grep.class, "Map-only job that performs Grep");
		addClass("moving_average",MovingAverage.class,"Moving average ");
		addClass("user_activity_normalizer",UserActivityNormalizer.class,"User activity normalizer");
	}
	
	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
