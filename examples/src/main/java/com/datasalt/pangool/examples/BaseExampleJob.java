package com.datasalt.pangool.examples;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class BaseExampleJob implements Tool, Configurable {
	
	public final static Charset UTF8 = Charset.forName("UTF-8");
	private String help;
	protected Configuration conf;
	
	public BaseExampleJob(String help) {
		this.help = help;
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	public void failArguments(String message) {
		System.err.println(message);
		System.err.println();
		ToolRunner.printGenericCommandUsage(System.out);
		System.out.println(help);
	}
	
	public void deleteOutput(String output) throws IOException {
		FileSystem.get(conf).delete(new Path(output), true);
	}
}
