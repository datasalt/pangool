package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.io.tuple.ITuple;

public class IdentityInputProcessor extends InputProcessor<ITuple, NullWritable> {

	@Override
	public void process(ITuple key, NullWritable value, Collector collector) throws IOException, InterruptedException {
		collector.write(key);
	}
}
