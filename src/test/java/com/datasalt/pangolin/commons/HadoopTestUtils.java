package com.datasalt.pangolin.commons;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangolin.commons.HadoopUtils;

public class HadoopTestUtils {

	/**
	 * Creates a file with just one line. Useful for test mapreduce jobs. It
	 * allows you create a mapper. The map function of this mapper will be called
	 * only once. This call can be used to emit whatever you need.  
	 * @throws IOException 
	 */
	public static void oneLineTextFile(FileSystem fs, Path path) throws IOException {
		HadoopUtils.stringToFile(fs, path, "Dummy Row");
	}
}
