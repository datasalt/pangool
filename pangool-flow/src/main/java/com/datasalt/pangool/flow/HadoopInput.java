package com.datasalt.pangool.flow;

import org.apache.hadoop.mapreduce.InputFormat;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

public class HadoopInput implements RichInput {

	InputFormat format;
	TupleMapper processor;
	Schema intermediateSchema;
	
	public HadoopInput(InputFormat format, TupleMapper processor, Schema intermediateSchema) {
		this.format = format;
	  this.processor = processor;
	  this.intermediateSchema = intermediateSchema;
	}
}
