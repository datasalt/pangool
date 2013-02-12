package com.datasalt.pangool.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat;

/**
 * A Pig's StoreFunc that can be used to save the result of a Pig flow into a Pangool-friendly format.
 * The Schema is lazily discovered with the first seen Pig Tuple. The type correspondence is:
 * <ul>
 *   <li>Integer - INT</li>
 *   <li>Long - LONG</li>
 *   <li>Float - FLOAT</li>
 *   <li>Double - DOUBLE</li>
 *   <li>String - STRING</li>
 *   <li>Short - INT</li>
 *   <li>Boolean - BOOLEAN</li>
 * </ul>
 * Any other type is unrecognized and an IOException is thrown.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PangoolStoreFunc extends StoreFunc {

	private RecordWriter writer;
	private ITuple pangoolTuple = null;
	
	private String schemaName;
	private String[] fieldNames;

	/**
	 * First argument is schema name, the others are the column names.
	 * We do it this way rather than (String, String...) because Pig doesn't recognize the constructor
	 * by reflection otherwise.
	 */
	public PangoolStoreFunc(String... args) {
		this.schemaName = args[0];
		this.fieldNames = new String[args.length - 1];
		for(int i = 1; i < args.length; i++) {
			fieldNames[i - 1] = args[i];
		}
	}
	
  @Override
  public OutputFormat getOutputFormat() throws IOException {
	  return new TupleOutputFormat();
  }

	@Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
  }

	@Override
  public void putNext(Tuple pigTuple) throws IOException {
		if(pangoolTuple == null) {
			List<Field> fields = new ArrayList<Field>();
			int i = 0;
			for(Object obj: pigTuple.getAll()) {
				if(obj instanceof Integer) {
					fields.add(Field.create(fieldNames[i], Field.Type.INT, true));
				} else if(obj instanceof Long) {
					fields.add(Field.create(fieldNames[i], Field.Type.LONG, true));
				} else if(obj instanceof Float) {
					fields.add(Field.create(fieldNames[i], Field.Type.FLOAT, true));
				} else if(obj instanceof Double) {
					fields.add(Field.create(fieldNames[i], Field.Type.DOUBLE, true));
				} else if(obj instanceof String) {
					fields.add(Field.create(fieldNames[i], Field.Type.STRING, true));
				} else if(obj instanceof Boolean) {
					fields.add(Field.create(fieldNames[i], Field.Type.BOOLEAN, true));
				} else if(obj instanceof Short) {
					fields.add(Field.create(fieldNames[i], Field.Type.INT, true));
				} else {
					throw new IOException("Can't handle type [" + obj.getClass() + "] - only primitive Java types allowed.");
				}
				i++;
			}
			Schema schema = new Schema(schemaName, fields);
			pangoolTuple = new com.datasalt.pangool.io.Tuple(schema);
		}
		
		int i = 0;
		for(Object obj: pigTuple.getAll()) {
			pangoolTuple.set(i, obj);
			i++;
		}
		
		try {
	    writer.write(pangoolTuple, NullWritable.get());
    } catch(InterruptedException e) {
	    throw new IOException(e);
    }
	}

	@Override
  public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
  }
}
