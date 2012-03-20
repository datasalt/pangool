package com.datasalt.pangool.flow;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMapper.TupleMRContext;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat.TupleInputReader;
import com.datasalt.pangool.utils.HadoopUtils;

public class Utils {

	public final static Tuple cacheTuple(Tuple tuple, @SuppressWarnings("rawtypes") TupleMRContext context, String schemaName) {
		if(tuple == null) {
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(schemaName));
		}
		return tuple;
	}
	
	public static void shallowCopy(ITuple tupleOrig, ITuple tupleDest) {
		for(Field field: tupleOrig.getSchema().getFields()) {
			tupleDest.set(field.getName(), tupleOrig.get(field.getName()));
		}
	}
	
	public static void delete(Path path, Configuration conf) throws IOException {
		HadoopUtils.deleteIfExists(path.getFileSystem(conf), path);
	}
	

	public abstract static class TupleVisitor {

		public abstract void onTuple(ITuple tuple);
	}

	public static class PrintVisitor extends TupleVisitor {

		@Override
    public void onTuple(ITuple tuple) {
	    System.out.println(tuple);
    }
	}
	
	/*
	 * Read the Tuples from a TupleOutput using TupleInputReader.
	 */
	public static void readTuples(String OUTPUT, TupleVisitor iterator) throws IOException, InterruptedException {
		File expectedOutputFile = new File(OUTPUT);
		Configuration configuration = new Configuration();
		TupleInputReader reader = new TupleInputReader(configuration);
		reader.initialize(new Path(expectedOutputFile + ""), configuration);
		while(reader.nextKeyValueNoSync()) {
			ITuple tuple = reader.getCurrentKey();
			iterator.onTuple(tuple);
		}
		reader.close();
	}
}