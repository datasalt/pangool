package com.datasalt.pangool.examples.tweets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import com.datasalt.pangool.examples.tweets.Beans.HashTag;
import com.datasalt.pangool.examples.tweets.Beans.SimpleTweet;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleRollupReducer;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.datasalt.pangool.utils.HadoopUtils;

public class TweetsExample {

	@SuppressWarnings("serial")
	private static class TweetsProcessor extends TupleMapper<LongWritable, Text> {

		private ObjectMapper jsonMapper;
		private Tuple tuple;

		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			jsonMapper = new ObjectMapper();
			jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema("my_schema"));
		}

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {

			for(Object rawTweet : jsonMapper.readValue(value.toString(), List.class)) { // For each tweet
				SimpleTweet tweet = jsonMapper.readValue(jsonMapper.writeValueAsString(rawTweet), SimpleTweet.class);
				if(tweet.getUser().getLocation() == null) {
					continue;
				}
				DateTime dateTime = new DateTime(tweet.getCreated_at_date());
				tuple.set("date", dateTime.getYear() + "-" + dateTime.getMonthOfYear() + "-" + dateTime.getDayOfMonth());
				tuple.set("location", tweet.getUser().getLocation());
				for(HashTag hashTag : tweet.getEntities().getHashtags()) {
					tuple.set("hashtag", hashTag.getText());
					tuple.set("count", 1);
					collector.write(tuple);
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static class TweetsHandler extends TupleRollupReducer<Text, NullWritable> {

		int totalCount = 0;

		public void onCloseGroup(int depth, String field, ITuple lastElement, com.datasalt.pangool.tuplemr.TupleReducer<Text,NullWritable>.TupleMRContext context, com.datasalt.pangool.tuplemr.TupleReducer<Text,NullWritable>.Collector collector) throws IOException ,InterruptedException ,TupleMRException {
		
			if(field.equals("hashtag")) {
				totalCount = 0;
			} else if(field.equals("date")) {
				// Flush top N
			}
		};
		
		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			for(ITuple tuple : tuples) {
				totalCount += (Integer) tuple.get("count");
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws TupleMRException, IOException {
		// Configure schema, sort and group by

		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("location",Type.STRING));
		fields.add(Field.create("date", Type.STRING));
		fields.add(Field.create("hashtag", Type.STRING));
		fields.add(Field.create("count",Type.INT));
		Schema schema = new Schema("my_schema", fields);

		TupleMRBuilder grouper = new TupleMRBuilder(conf);
		grouper.addIntermediateSchema(schema);
		grouper.setOrderBy(new OrderBy().add("location", Order.ASC).add("date", Order.ASC).add("hashtag", Order.ASC));
		grouper.setGroupByFields("location", "date", "hashtag");
		grouper.setRollupFrom("date");
		// Input / output and such
		grouper.setTupleReducer(new TweetsHandler());
		grouper.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		grouper.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new TweetsProcessor());
		return grouper.createJob();
	}

	private static final String HELP = "Usage: [input_path] [output_path]";

	public static void main(String args[]) throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(args[1]));
		new TweetsExample().getJob(conf, args[0], args[1]).waitForCompletion(true);
	}
}
