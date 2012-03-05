package com.datasalt.pangool.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat.TupleInputReader;

public class TupleViewer {

	public static void main(String[] args) throws IOException, InterruptedException {
		if(args.length != 1 && args.length != 2) {
			System.err.println("Usage: [path_or_glob]. Optionally add local as second argument to use local filesytem." );
			System.exit(-1);
		}
		Path path = new Path(args[0]);
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		if(args.length == 2 && args[1].equals("local")) {
			fS = FileSystem.getLocal(conf);
		}
		
		for(FileStatus fStatus: fS.globStatus(path)) {
			TupleInputReader reader = new TupleInputReader(conf);
			reader.initialize(fStatus.getPath(), conf);
			while(reader.nextKeyValueNoSync()) {
				System.out.println(fStatus.getPath() + "\t" + reader.getCurrentKey());
			}
		}
	}
}
