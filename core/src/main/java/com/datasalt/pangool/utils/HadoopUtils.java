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

package com.datasalt.pangool.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * <p>
 * Put here usefull methods for manipulating things in the HDFS, etc.
 * </p>
 * 
 */
public class HadoopUtils {

	public static void deleteIfExists(FileSystem dFs, Path path) throws IOException {
		if(dFs.exists(path)) {
			dFs.delete(path, true);
		}
	}

	public static void synchronize(FileSystem fS1, Path p1, FileSystem fS2, Path p2)
	    throws IOException {
		deleteIfExists(fS2, p2);
		FileUtil.copy(fS1, p1, fS2, p2, false, false, fS1.getConf());
	}

	/**
	 * Creates a file with the given string, overwritting if needed.
	 */
	public static void stringToFile(FileSystem fs, Path path, String string)
	    throws IOException {
		OutputStream os = fs.create(path, true);
		PrintWriter pw = new PrintWriter(os);
		pw.append(string);
		pw.close();
	}

	/**
	 * Reads the content of a file into a String. Return null if the file does not
	 * exist.
	 */
	public static String fileToString(FileSystem fs, Path path) throws IOException {
		if(!fs.exists(path)) {
			return null;
		}

		InputStream is = fs.open(path);
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		char[] buff = new char[256];
		StringBuilder sb = new StringBuilder();
		int read;
		while((read = br.read(buff)) != -1) {
			sb.append(buff, 0, read);
		}
		br.close();
		return sb.toString();
	}

	/**
	 * Reads maps of integer -> double
	 */
	public static HashMap<Integer, Double> readIntDoubleMap(Path path, FileSystem fs)
	    throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());

		IntWritable topic = new IntWritable();
		DoubleWritable value = new DoubleWritable();

		HashMap<Integer, Double> ret = new HashMap<Integer, Double>();

		while(reader.next(topic)) {
			reader.getCurrentValue(value);

			ret.put(topic.get(), value.get());
		}

		reader.close();
		return ret;
	}

	/**
	 * Reads maps of integer -> double from glob paths like "folder/part-r*"
	 */
	public static HashMap<Integer, Double> readIntDoubleMapFromGlob(Path glob, FileSystem fs)
	    throws IOException {
		FileStatus status[] = fs.globStatus(glob);
		HashMap<Integer, Double> ret = new HashMap<Integer, Double>();
		for(FileStatus fileS : status) {
			ret.putAll(readIntDoubleMap(fileS.getPath(), fs));
		}
		return ret;
	}

	/**
	 * Reads maps of integer -> integer
	 */
	public static HashMap<Integer, Integer> readIntIntMap(Path path, FileSystem fs)
	    throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());

		IntWritable topic = new IntWritable();
		IntWritable value = new IntWritable();

		HashMap<Integer, Integer> ret = new HashMap<Integer, Integer>();

		while(reader.next(topic)) {
			reader.getCurrentValue(value);

			ret.put(topic.get(), value.get());
		}

		reader.close();
		return ret;
	}

	/**
	 * Reads maps of integer -> integer from glob paths like "folder/part-r*"
	 */
	public static HashMap<Integer, Integer> readIntIntMapFromGlob(Path glob, FileSystem fs)
	    throws IOException {
		FileStatus status[] = fs.globStatus(glob);
		HashMap<Integer, Integer> ret = new HashMap<Integer, Integer>();
		for(FileStatus fileS : status) {
			ret.putAll(readIntIntMap(fileS.getPath(), fs));
		}
		return ret;
	}

	/**
	 * Utility for doing ctx.getCounter(groupName,
	 * counter.toString()).increment(1);
	 */
	@SuppressWarnings("rawtypes")
	public static void incCounter(TaskInputOutputContext ctx, String groupName, Enum counter) {
		ctx.getCounter(groupName, counter.toString()).increment(1);
	}
}