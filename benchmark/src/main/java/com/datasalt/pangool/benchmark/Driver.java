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
package com.datasalt.pangool.benchmark;

import org.apache.hadoop.util.ProgramDriver;

import com.datasalt.pangool.benchmark.cogroup.CascadingUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.CrunchUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.HadoopUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.PangoolUrlResolution;
import com.datasalt.pangool.benchmark.sansaccentsorting.SansAccentRepeatedField;
import com.datasalt.pangool.benchmark.secondarysort.CascadingSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.CrunchSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.HadoopSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.PangoolSecondarySort;
import com.datasalt.pangool.benchmark.wordcount.CrunchWordCount;
import com.datasalt.pangool.benchmark.wordcount.HadoopWordCount;
import com.datasalt.pangool.benchmark.wordcount.PangoolWordCount;
import com.datasalt.pangool.benchmark.wordcount.CascadingWordCount;

public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();
		addClass("hadoop-wordcount", HadoopWordCount.class, "Typical word count in Hadoop Map/Red API");
		addClass("crunch-wordcount", CrunchWordCount.class, "Typical word count in Crunch");
		addClass("cascading-wordcount", CascadingWordCount.class, "Typical word count in Cascading");
		// ----------- //
		addClass("hadoop-secondarysort", HadoopSecondarySort.class, "Secondary sort example (Hadoop Map/Red API)");
		addClass("crunch-secondarysort", CrunchSecondarySort.class, "Secondary sort example (Crunch / Avro)");
		addClass("cascading-secondarysort", CascadingSecondarySort.class, "Secondary sort example (Cascading)");
		// ----------- //
		addClass("hadoop-urlresolution", HadoopUrlResolution.class, "URL Resolution CoGroup (Hadoop Map/Red API)");
		addClass("crunch-urlresolution", CrunchUrlResolution.class, "URL Resolution CoGroup (Crunch / Avro)");
		addClass("cascading-urlresolution", CascadingUrlResolution.class, "URL Resolution CoGroup (Cascading)");
		
		addClass("accent-sorting-repeated",SansAccentRepeatedField.class,"Utf8 accent sorting using repeated field with removed accents");
		//addClass("accent-sorting-repeated",LargestWordBytesRepeatedField.class,"Utf8 accent sorting using repeated field with removed accents");

	}
	
	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
