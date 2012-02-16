package com.datasalt.pangool.benchmark;

import org.apache.hadoop.util.ProgramDriver;

import com.datasalt.pangool.benchmark.cogroup.CascadingUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.CrunchUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.HadoopUrlResolution;
import com.datasalt.pangool.benchmark.cogroup.PangoolUrlResolution;
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
		addClass("pangool-wordcount", PangoolWordCount.class, "Typical word count in Pangool");
		addClass("hadoop-wordcount", HadoopWordCount.class, "Typical word count in Hadoop Map/Red API");
		addClass("crunch-wordcount", CrunchWordCount.class, "Typical word count in Crunch");
		addClass("cascading-wordcount", CascadingWordCount.class, "Typical word count in Cascading");
		// ----------- //
		addClass("pangool-secondarysort", PangoolSecondarySort.class, "Secondary sort example (Pangool)");
		addClass("hadoop-secondarysort", HadoopSecondarySort.class, "Secondary sort example (Hadoop Map/Red API)");
		addClass("crunch-secondarysort", CrunchSecondarySort.class, "Secondary sort example (Crunch / Avro)");
		addClass("cascading-secondarysort", CascadingSecondarySort.class, "Secondary sort example (Cascading)");
		// ----------- //
		addClass("pangool-urlresolution", PangoolUrlResolution.class, "URL Resolution CoGroup (Pangool)");
		addClass("hadoop-urlresolution", HadoopUrlResolution.class, "URL Resolution CoGroup (Hadoop Map/Red API)");
		addClass("crunch-urlresolution", CrunchUrlResolution.class, "URL Resolution CoGroup (Crunch / Avro)");
		addClass("cascading-urlresolution", CascadingUrlResolution.class, "URL Resolution CoGroup (Cascading)");
	}
	
	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
