package com.datasalt.pangool.benchmark.cogroup;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import com.google.common.io.Files;

public abstract class BaseBenchmarkTest {

	
	public String getReducerOutputAsText(String outputDir) throws IOException {
		return getOutputAsText(outputDir + "/part-r-00000");
	}
	
	public String getOutputAsText(String outFile) throws IOException {
		return Files.toString(new File(outFile), Charset.forName("UTF-8"));
	}
	
}
