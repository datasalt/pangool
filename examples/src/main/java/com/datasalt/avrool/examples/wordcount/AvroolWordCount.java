package com.datasalt.avrool.examples.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.avrool.CoGrouper;
import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Ordering;
import com.datasalt.avrool.PangoolKey;
import com.datasalt.avrool.api.CombinerHandler;
import com.datasalt.avrool.api.GroupHandler;
import com.datasalt.avrool.api.GroupHandler.CoGrouperContext;
import com.datasalt.avrool.api.GroupHandler.Collector;
import com.datasalt.avrool.api.InputProcessor;
import com.datasalt.avrool.commons.HadoopUtils;



public class AvroolWordCount {

	private static final String WORD_FIELD = "word";
	private static final String COUNT_FIELD = "count";
	private static final String SCHEMA_NAME = "s1";

	@SuppressWarnings("serial")
	private static class Split extends InputProcessor<LongWritable, Text> {

		private Record outputRecord;

		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			CoGrouperConfig grouperConfig = context.getPangoolConfig();
			Schema schema = grouperConfig.getSchemaBySource(SCHEMA_NAME);
			outputRecord = new Record(schema);
			outputRecord.put(1,1);
		}
		
		
		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			//outputRecord.put(COUNT_FIELD, 1);
			
			while(itr.hasMoreTokens()) {
//				Utf8 utf8 = new Utf8();
//				utf8.
				
				//outputRecord.put(WORD_FIELD, itr.nextToken());
				outputRecord.put(0,itr.nextToken());
				collector.write(outputRecord);
			}
		}
	}

	@SuppressWarnings("serial")
	private static class CountCombiner extends CombinerHandler {

		private Record outputRecord;

		public void setup(CoGrouperContext<PangoolKey,NullWritable> context, Collector collector) throws IOException, InterruptedException, CoGrouperException {
			CoGrouperConfig grouperConfig = context.getPangoolConfig();
			Schema schema = grouperConfig.getSchemaBySource(SCHEMA_NAME);
			outputRecord = new Record(schema);
		}
		
		
		@Override
		public void onGroupElements(GenericRecord group, Iterable<GenericRecord> values, CoGrouperContext<PangoolKey,NullWritable> context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			int count = 0;
			//this.outputRecord.put(WORD_FIELD, group.get(WORD_FIELD));
			this.outputRecord.put(0, group.get(0));
			for(GenericRecord value : values) {
				//count += (Integer)tuple.get(COUNT_FIELD);
				count += (Integer)value.get(1);
			}
			//this.outputRecord.put(COUNT_FIELD, count);
			this.outputRecord.put(1, count);
			collector.write(this.outputRecord);
		}
	}

	@SuppressWarnings("serial")
	private static class Count extends GroupHandler<Text, IntWritable> {

		private Text outputKey=new Text();
		private IntWritable outputValue = new IntWritable();
		
		@Override
		public void onGroupElements(GenericRecord group, Iterable<GenericRecord> tuples, CoGrouperContext<Text,IntWritable> context, Collector<Text,IntWritable> collector)
		    throws IOException, InterruptedException, CoGrouperException {
			int count = 0;
			for(GenericRecord tuple : tuples) {
				//count += (Integer)tuple.get(COUNT_FIELD);
				count += (Integer)tuple.get(1);
			}
			//collector.write(new Text(group.get(WORD_FIELD).toString()), new IntWritable(count));
			Utf8 utf8 = (Utf8)group.get(0);
			outputKey.set(utf8.getBytes(),0,utf8.getByteLength());
			outputValue.set(count);
			collector.write(outputKey,outputValue);
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException, IOException{
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		CoGrouperConfigBuilder b = CoGrouperConfigBuilder.newOne();
		
		List<Field> userFields = new ArrayList<Field>();
		
		userFields.add(new Field(WORD_FIELD, Schema.create(Type.STRING),null,null));
		userFields.add(new Field(COUNT_FIELD, Schema.create(Type.INT),null,null));
		
		Schema schema = Schema.createRecord(SCHEMA_NAME, null,null, false);
		schema.setFields(userFields);

		b.addSource(schema);
		b.setGroupByFields(WORD_FIELD);
		b.setCommonOrdering(new Ordering().add(WORD_FIELD,Order.ASCENDING));
		
		CoGrouperConfig config = b.build();
		CoGrouper cg = new CoGrouper(config, conf);
		cg.setJarByClass(AvroolWordCount.class);
		
		cg.addInput(new Path(input), TextInputFormat.class, Split.class);
		cg.setOutput(new Path(output), TextOutputFormat.class, Text.class, Text.class);
		cg.setGroupHandler(Count.class);
		cg.setCombinerHandler(CountCombiner.class);

		Path outputPath = new Path(output);
		HadoopUtils.deleteIfExists(fs, outputPath);
		
		return cg.createJob();
	}

	private static final String HELP = "Usage: WordCount [input_path] [output_path]";

	public static void main(String args[]) throws CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		new AvroolWordCount().getJob(conf, args[0], args[1]).waitForCompletion(true);
	}
}

