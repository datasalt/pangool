package com.datasalt.pangool.flow.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.datasalt.pangool.flow.ops.TupleParser;

@SuppressWarnings("serial")
public class ParseMapper extends SingleSchemaMapper<LongWritable, Text> {

	TupleParser parser;

	public ParseMapper(TupleParser parser) {
		super(parser.getSchema());
		this.parser = parser;
	}

	@Override
	public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
	    InterruptedException {
		collector.write(parser.parse(value.toString()));
	}
}
