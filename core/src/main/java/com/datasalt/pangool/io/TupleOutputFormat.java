package com.datasalt.pangool.io;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;

public class TupleOutputFormat extends FileOutputFormat<ITuple, NullWritable> {
	
  public static final String DEFLATE_CODEC = "deflate";
  public static final String SNAPPY_CODEC = "snappy";

  private static final int SYNC_SIZE = 16;
  private static final int DEFAULT_SYNC_INTERVAL = 1000*SYNC_SIZE; 
  
	public static class TupleRecordWriter extends RecordWriter<ITuple, NullWritable> {

		AvroWrapper<Record> wrapper = new AvroWrapper<Record>();
		DataFileWriter<Record> writer;
		Schema pangoolSchema;
		
		public TupleRecordWriter(org.apache.avro.Schema schema, Schema pangoolSchema, DataFileWriter<Record> writer) {
			wrapper.datum(new Record(schema));
			this.writer = writer;
			this.pangoolSchema = pangoolSchema;
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			writer.close();
		}

		@Override
		public void write(ITuple tuple, NullWritable ignore) throws IOException, InterruptedException {
			// Convert Tuple to Record
			for(Field field: pangoolSchema.getFields()) {
				wrapper.datum().put(field.getName(), tuple.getObject(field.getName()));
			}

			writer.append(wrapper.datum());
		}
	}

	private String getOutFileName(TaskAttemptContext context, String prefix) {
		TaskID taskId = context.getTaskAttemptID().getTaskID();
		int partition = taskId.getId();
		NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);
		StringBuilder result = new StringBuilder();
		result.append(prefix);
		result.append("-");
		result.append(nf.format(partition));
		return result.toString();
	}

	@Override
	public RecordWriter<ITuple, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
	    InterruptedException {


		List<org.apache.avro.Schema.Field> avroFields = new ArrayList<org.apache.avro.Schema.Field>();
		Schema pangoolOutputSchema;
    try {
	    pangoolOutputSchema = Schema.parse(context.getConfiguration().get("output_schema"));
    } catch(InvalidFieldException e) {
	    throw new RuntimeException(e);
    } catch(CoGrouperException e) {
	    throw new RuntimeException(e);
    }
		for(Field field: pangoolOutputSchema.getFields()) {
			org.apache.avro.Schema fieldsSchema = null;
			if(field.getType().equals(String.class)) {
				fieldsSchema = org.apache.avro.Schema.create(Type.STRING);
			}
			avroFields.add(new org.apache.avro.Schema.Field(field.getName(), fieldsSchema, null, null));
		}

		org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("pangool", null, null, false);
		avroSchema.setFields(avroFields);
		DataFileWriter<Record> writer = new DataFileWriter<Record>(new ReflectDatumWriter<Record>());

		// Compression etc
		
		Configuration conf = context.getConfiguration();
		if(conf.getBoolean("mapred.output.compress", false)) {
			String codec = conf.get("mapred.output.compression");
			int level = conf.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, AvroOutputFormat.DEFAULT_DEFLATE_LEVEL);
			CodecFactory factory = codec.equals(DEFLATE_CODEC) ? CodecFactory.deflateCodec(level) : CodecFactory
			    .fromString(codec);
			writer.setCodec(factory);
		}

		writer.setSyncInterval(conf.getInt(AvroOutputFormat.SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL));

		Path path = new Path(FileOutputFormat.getOutputPath(context), getOutFileName(context, "part"));
		writer.create(avroSchema, path.getFileSystem(context.getConfiguration()).create(path));

		return new TupleRecordWriter(avroSchema, pangoolOutputSchema, writer);
	}
}
