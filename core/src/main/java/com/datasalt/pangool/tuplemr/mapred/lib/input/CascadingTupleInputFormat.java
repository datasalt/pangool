package com.datasalt.pangool.tuplemr.mapred.lib.input;

import static cascading.tuple.hadoop.TupleSerializationProps.HADOOP_IO_SERIALIZATIONS;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.Util;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;

/**
 * A wrapper around a SequenceFile that contains Cascading's Tuples that implements a Pangool-friendly InputFormat.
 * The Schema is lazily discovered with the first seen Cascading Tuple. The type correspondence is:
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
 * <p>
 * Column names must be provided to the InputFormat, this is of course because Cascading doesn't save them anywhere.
 * The schemaName is used to instantiate a Pangool Schema.
 * <p>
 * Note that for this to work Cascading serialization must have been enabled in Hadoop Configuration.
 * You can do this by calling static method {@link #setSerializations(Configuration)}.
 */
@SuppressWarnings("serial")
public class CascadingTupleInputFormat extends FileInputFormat<ITuple, NullWritable> implements Serializable {

	private static Logger log = LoggerFactory.getLogger(CascadingTupleInputFormat.class);
	
	private String schemaName;
	private String[] fieldNames;

	public CascadingTupleInputFormat(String schemaName, String... fieldNames) {
		this.schemaName = schemaName;
		this.fieldNames = fieldNames;
	}

	/**
	 * Like in Cascading's TupleSerialization.setSerializations() but accepting a Hadoop's Configuration rather than JobConf.
	 */
	public static void setSerializations(Configuration conf) {
		String serializations = conf.get(HADOOP_IO_SERIALIZATIONS);

		LinkedList<String> list = new LinkedList<String>();

		if(serializations != null && !serializations.isEmpty())
			Collections.addAll(list, serializations.split(","));

		// required by MultiInputSplit
		String writable = WritableSerialization.class.getName();
		String tuple = TupleSerialization.class.getName();

		list.remove(writable);
		list.remove(tuple);

		list.addFirst(writable);
		list.addFirst(tuple);

		// make writable last
		conf.set(HADOOP_IO_SERIALIZATIONS, Util.join(list, ","));
	}
	
	@Override
	public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx)
	    throws IOException, InterruptedException {

		return new RecordReader<ITuple, NullWritable>() {

			RecordReader<cascading.tuple.Tuple, cascading.tuple.Tuple> delegatingRecordReader;
			ITuple tuple;

			@Override
			public void close() throws IOException {
			}

			@Override
			public ITuple getCurrentKey() throws IOException, InterruptedException {
				cascading.tuple.Tuple cTuple = delegatingRecordReader.getCurrentValue();
				
				if(tuple == null) {
					int i = 0;
					List<Field> fields = new ArrayList<Field>();
					
					for(Class<?> cl : cTuple.getTypes()) {
						if(cl.equals(Integer.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.INT, true));
						} else if(cl.equals(Long.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.LONG, true));
						} else if(cl.equals(Float.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.FLOAT, true));
						} else if(cl.equals(Double.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.DOUBLE, true));
						} else if(cl.equals(String.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.STRING, true));
						} else if(cl.equals(Boolean.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.BOOLEAN, true));
						} else if(cl.equals(Short.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.INT, true));
						} else {
							throw new IOException("Can't handle type [" + cl + "] - only primitive Java types allowed.");
						}
						i++;
					}
					Schema schema = new Schema(schemaName, fields);
					log.info("Lazily instantiated a Pangool Schema from Cascading Tuple: [" + schema + "]");
					tuple = new Tuple(schema);
				}
				
				// Just perform a normal Object copying - without checking Schema everytime.
				// This is more efficient but it will raise errors later.
				for(int i = 0; i < tuple.getSchema().getFields().size(); i++) {
					tuple.set(i, cTuple.getObject(i));
				}
				
				return tuple;
			}

			@Override
			public NullWritable getCurrentValue() throws IOException, InterruptedException {
				return NullWritable.get();
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return delegatingRecordReader.getProgress();
			}

			@Override
			public void initialize(InputSplit iS, TaskAttemptContext ctx) throws IOException,
			    InterruptedException {
				delegatingRecordReader = new SequenceFileInputFormat<cascading.tuple.Tuple, cascading.tuple.Tuple>()
				    .createRecordReader(iS, ctx);
				delegatingRecordReader.initialize(iS, ctx);
			}

			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				return delegatingRecordReader.nextKeyValue();
			}
		};
	}
}