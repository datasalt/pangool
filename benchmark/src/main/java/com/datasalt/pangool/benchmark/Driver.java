package com.datasalt.pangool.benchmark;

import org.apache.hadoop.util.ProgramDriver;

import com.datasalt.pangool.benchmark.cogroup.CascadingUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.CrunchUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.MapRedUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.UrlResolution;
import com.datasalt.pangool.benchmark.secondarysort.CascadingSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.CrunchSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.MapredSecondarySort;
import com.datasalt.pangool.benchmark.secondarysort.SecondarySort;
import com.datasalt.pangool.benchmark.wordcount.MapRedWordCount;
import com.datasalt.pangool.benchmark.wordcount.WordCount;
import com.datasalt.pangool.benchmark.wordcount.CascadingWordCount;

public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();
		addClass("wordcount", WordCount.class, "Typical word count in Pangool");
		addClass("mapred-wordcount", MapRedWordCount.class, "Typical word count in Hadoop Map/Red API");
		addClass("crunch-wordcount", MapRedWordCount.class, "Typical word count in Crunch");
		addClass("cascading-wordcount", CascadingWordCount.class, "Typical word count in Cascading");
		// ----------- //
		addClass("secondarysort", SecondarySort.class, "Secondary sort example (Pangool)");
		addClass("mapred-secondarysort", MapredSecondarySort.class, "Secondary sort example (Hadoop Map/Red API)");
		addClass("crunch-secondarysort", CrunchSecondarySort.class, "Secondary sort example (Crunch / Avro)");
		addClass("cascading-secondarysort", CascadingSecondarySort.class, "Secondary sort example (Cascading)");
		// ----------- //
		addClass("urlresolution", UrlResolution.class, "URL Resolution CoGroup (Pangool)");
		addClass("mapred-urlresolution", MapRedUrlResolution.class, "URL Resolution CoGroup (Hadoop Map/Red API)");
		addClass("crunch-urlresolution", CrunchUrlResolution.class, "URL Resolution CoGroup (Crunch / Avro)");
		addClass("cascading-urlresolution", CascadingUrlResolution.class, "URL Resolution CoGroup (Cascading)");
	}
	
	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
